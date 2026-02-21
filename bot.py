"""
Tentacles Bot - Многофункциональный Telegram бот для работы с видео
Объединяет: склейку, скачивание и сжатие видео (2 варианта)
Вариант 1: FFmpeg (H.264, H.265, AV1) — локальное сжатие до нужного размера
Вариант 2: ezgif.com — облачное сжатие с выбором разрешения/битрейта/формата
"""
import asyncio
import json
import logging
import math
import os
import tempfile
import uuid
import shutil
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import aiofiles
import aiohttp
import httpx
import yt_dlp
import requests
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from compressor import compress_video, get_video_info

# Load environment variables
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")

TEMPSHARE_API = "https://api.tempshare.su/upload"
TEMPSHARE_DURATION = 3  # days

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Concurrency settings
FFMPEG_SEMAPHORE = asyncio.Semaphore(3)
FFMPEG_THREADS = str(os.cpu_count() or 4)

# Memory safeguards
MAX_CONCURRENT_JOBS = 1
MAX_DOWNLOAD_SIZE_MB = 1024
DOWNLOAD_CHUNK_SIZE = 256 * 1024

_job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
active_compress_jobs: Dict[int, dict] = {}
active_compress_tasks: Dict[int, asyncio.Task] = {}

# ===================== EZGIF CONFIGURATION =====================

EZGIF_BASE = "https://ezgif.com"
EZGIF_COMPRESS_URL = f"{EZGIF_BASE}/video-compressor"
TEMPSHARE_UPLOAD_URL = "https://api.tempshare.su/upload"
TEMPSHARE_EZGIF_DURATION = 7

EZGIF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": EZGIF_COMPRESS_URL,
}

RESOLUTIONS = {
    "original": "Оригинал",
    "1920x1080": "1920x1080 (Full HD)",
    "1280x720": "1280x720 (HD)",
    "854x480": "854x480 (SD)",
    "640x360": "640x360 (360p)",
    "426x240": "426x240 (240p)",
}

BITRATES = {
    "1000": "1000 kbps (высокое)",
    "750": "750 kbps (хорошее)",
    "500": "500 kbps (среднее)",
    "300": "300 kbps (низкое)",
    "150": "150 kbps (минимальное)",
}

EZGIF_FORMATS = {
    "mp4": "MP4",
    "webm": "WebM",
    "mkv": "MKV",
}

# Thread pool for blocking ezgif HTTP calls
ezgif_executor = ThreadPoolExecutor(max_workers=4)

# Pending ezgif requests: chat_id -> {url, resolution, bitrate}
ezgif_pending: Dict[int, dict] = {}


# ===================== UTILITY FUNCTIONS =====================

def format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def format_duration(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def format_expires(expires_str: str) -> str:
    """Convert ISO date to human readable format"""
    try:
        if 'T' in expires_str:
            dt = datetime.fromisoformat(expires_str.replace('Z', '+00:00'))
            return dt.strftime('%d.%m.%Y %H:%M')
        return expires_str
    except:
        return expires_str


def is_url(text: str) -> bool:
    return text.startswith("http://") or text.startswith("https://")


def _fmt_time(seconds: float) -> str:
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m} мин {s} сек" if m > 0 else f"{s} сек"


# ===================== MERGE (СКЛЕЙКА) =====================

@dataclass
class MergeUserData:
    """User session data for merging"""
    videos: List[str] = field(default_factory=list)
    watermark_path: Optional[str] = None
    watermark_size: int = 15
    watermark_speed: float = 50.0
    temp_dir: Optional[str] = None


merge_sessions: Dict[int, MergeUserData] = {}


class MergeStates(StatesGroup):
    collecting_videos = State()
    waiting_for_watermark = State()


def get_merge_data(user_id: int) -> MergeUserData:
    if user_id not in merge_sessions:
        temp_dir = tempfile.mkdtemp(prefix=f"tentacles_merge_{user_id}_")
        merge_sessions[user_id] = MergeUserData(temp_dir=temp_dir)
    return merge_sessions[user_id]


def cleanup_merge_data(user_id: int):
    if user_id in merge_sessions:
        data = merge_sessions[user_id]
        if data.temp_dir and os.path.exists(data.temp_dir):
            shutil.rmtree(data.temp_dir, ignore_errors=True)
        del merge_sessions[user_id]


def clear_merge_videos(user_id: int):
    if user_id not in merge_sessions:
        return
    data = merge_sessions[user_id]
    for video_path in data.videos:
        if os.path.exists(video_path):
            try:
                os.remove(video_path)
            except OSError:
                pass
    if data.temp_dir and os.path.exists(data.temp_dir):
        for f in os.listdir(data.temp_dir):
            if f.startswith(("normalized_", "concatenated", "merged_", "videos.txt")):
                try:
                    os.remove(os.path.join(data.temp_dir, f))
                except OSError:
                    pass
    data.videos = []


async def run_ffmpeg(cmd: list, description: str = "ffmpeg") -> tuple:
    async with FFMPEG_SEMAPHORE:
        logger.info(f"Running {description}: {' '.join(cmd[:5])}...")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            error_msg = stderr.decode(errors='replace')
            logger.error(f"{description} failed (rc={process.returncode}): {error_msg}")
            # Skip ffmpeg version/config header, show only last 800 chars (actual error)
            raise RuntimeError(f"{description} failed: {error_msg[-800:]}")
        return stdout, stderr


async def probe_video(video_path: str) -> dict:
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height,duration',
        '-show_entries', 'format=duration',
        '-of', 'csv=p=0',
        video_path
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await process.communicate()
    lines = stdout.decode().strip().split('\n')
    
    width, height, duration = 1280, 720, 60.0
    try:
        parts = lines[0].strip().split(',')
        width = int(parts[0])
        height = int(parts[1])
        if len(parts) > 2 and parts[2] and parts[2] != 'N/A':
            duration = float(parts[2])
        elif len(lines) > 1 and lines[1].strip() and lines[1].strip() != 'N/A':
            duration = float(lines[1].strip())
    except (ValueError, IndexError):
        pass
    
    return {'width': width, 'height': height, 'duration': duration}


async def probe_image(image_path: str) -> dict:
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=width,height',
        '-of', 'csv=p=0',
        image_path
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await process.communicate()
    
    width, height = 100, 100
    try:
        parts = stdout.decode().strip().split(',')
        width = int(parts[0])
        height = int(parts[1])
    except (ValueError, IndexError):
        pass
    
    return {'width': width, 'height': height}


async def normalize_single_video(i: int, video_path: str, temp_dir: str) -> str:
    normalized_path = os.path.join(temp_dir, f"normalized_{i}.mp4")
    cmd = [
        'ffmpeg', '-y',
        '-threads', FFMPEG_THREADS,
        '-i', video_path,
        '-map', '0:v:0',
        '-map', '0:a:0?',          # ? — аудио опционально (видео без звука не падает)
        '-c:v', 'libx264', '-preset', 'ultrafast',
        '-c:a', 'aac', '-ar', '44100', '-ac', '2',
        '-vf', 'scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,setsar=1',
        '-r', '30',
        '-max_muxing_queue_size', '2048',
        '-movflags', '+faststart',
        normalized_path
    ]
    await run_ffmpeg(cmd, f"normalize video {i+1}")
    return normalized_path


async def merge_videos_process(data: MergeUserData, progress_callback=None) -> str:
    if len(data.videos) < 1:
        raise ValueError("Нужно минимум 1 видео для обработки")
    
    output_path = os.path.join(data.temp_dir, f"merged_{uuid.uuid4().hex[:8]}.mp4")
    list_file = os.path.join(data.temp_dir, "videos.txt")
    
    if progress_callback:
        await progress_callback(f"Обработка {len(data.videos)} видео...")
    
    tasks = [
        normalize_single_video(i, video_path, data.temp_dir)
        for i, video_path in enumerate(data.videos)
    ]
    normalized_videos = await asyncio.gather(*tasks)
    
    async with aiofiles.open(list_file, 'w') as f:
        for video_path in normalized_videos:
            await f.write(f"file '{video_path}'\n")
    
    if progress_callback:
        await progress_callback("Склеивание видео...")
    
    concat_output = os.path.join(data.temp_dir, "concatenated.mp4")
    concat_cmd = [
        'ffmpeg', '-y',
        '-threads', FFMPEG_THREADS,
        '-f', 'concat', '-safe', '0',
        '-i', list_file,
        '-c', 'copy',
        '-movflags', '+faststart',
        concat_output
    ]
    
    await run_ffmpeg(concat_cmd, "concat videos")
    
    if data.watermark_path and os.path.exists(data.watermark_path):
        if progress_callback:
            await progress_callback("Добавление водяного знака...")
        
        video_info = await probe_video(concat_output)
        video_width = video_info['width']
        video_height = video_info['height']
        
        wm_info = await probe_image(data.watermark_path)
        wm_orig_w = wm_info['width']
        wm_orig_h = wm_info['height']
        
        wm_height = max(2, (int(video_height * data.watermark_size / 100) // 2) * 2)
        wm_width = int(wm_orig_w * wm_height / max(wm_orig_h, 1))
        wm_width = max(2, (wm_width // 2) * 2)
        
        max_wm_width = video_width - 4
        max_wm_width = max(2, (max_wm_width // 2) * 2)
        if wm_width > max_wm_width:
            wm_width = max_wm_width
            wm_height = int(wm_orig_h * wm_width / max(wm_orig_w, 1))
            wm_height = max(2, (wm_height // 2) * 2)
        
        range_x = max(1, video_width - wm_width)
        range_y = max(1, video_height - wm_height)
        
        speed = data.watermark_speed
        speed_y = speed * 0.7
        
        watermark_filter = (
            f"[1:v]scale={wm_width}:{wm_height},format=rgba[wm];"
            f"[0:v][wm]overlay="
            f"x='abs(mod(t*{speed},{range_x * 2})-{range_x})':"
            f"y='abs(mod(t*{speed_y},{range_y * 2})-{range_y})'"
        )
        
        watermark_cmd = [
            'ffmpeg', '-y',
            '-threads', FFMPEG_THREADS,
            '-i', concat_output,
            '-i', data.watermark_path,
            '-filter_complex', watermark_filter,
            '-c:v', 'libx264', '-preset', 'ultrafast',
            '-c:a', 'copy',
            '-max_muxing_queue_size', '2048',
            '-movflags', '+faststart',
            output_path
        ]
        
        await run_ffmpeg(watermark_cmd, "apply watermark")
    else:
        shutil.copy(concat_output, output_path)
    
    return output_path


# ===================== DOWNLOAD (СКАЧИВАНИЕ) =====================

class DownloadStates(StatesGroup):
    waiting_for_url = State()


async def download_video_ytdlp(url: str, download_dir: str) -> str | None:
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "outtmpl": os.path.join(download_dir, "%(title).80s.%(ext)s"),
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 30,
        "retries": 3,
    }

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _sync_download, url, ydl_opts)
        return result
    except Exception as e:
        logger.error(f"Download error: {e}")
        return None


def _sync_download(url: str, ydl_opts: dict) -> str | None:
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if info is None:
            return None
        filename = ydl.prepare_filename(info)
        base, _ = os.path.splitext(filename)
        mp4_path = base + ".mp4"
        if os.path.exists(mp4_path):
            return mp4_path
        if os.path.exists(filename):
            return filename
        download_dir = os.path.dirname(filename)
        for f in os.listdir(download_dir):
            fpath = os.path.join(download_dir, f)
            if os.path.isfile(fpath):
                return fpath
        return None


# ===================== COMPRESS (СЖАТИЕ) — Вариант 1: FFmpeg =====================

class CompressStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_size = State()
    waiting_for_custom_size = State()
    waiting_for_codec = State()


# ===================== COMPRESS (СЖАТИЕ) — Вариант 2: ezgif =====================

class EzgifStates(StatesGroup):
    waiting_for_url = State()
    waiting_for_resolution = State()
    waiting_for_bitrate = State()
    waiting_for_format = State()


# ===================== EZGIF PIPELINE (синхронные функции) =====================

def ezgif_step1_upload_url(video_url: str) -> dict:
    session = requests.Session()
    session.headers.update(EZGIF_HEADERS)
    data = {
        "new-image-url": video_url,
        "upload": "Upload video!",
    }
    logger.info(f"ezgif Step 1: Uploading URL: {video_url}")
    resp = session.post(EZGIF_COMPRESS_URL, data=data, timeout=120)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    file_input = soup.find("input", {"name": "file"})
    if not file_input:
        error = soup.find("p", class_="error")
        if error:
            raise Exception(f"ezgif error: {error.get_text(strip=True)}")
        raise Exception("Не удалось найти file ID в ответе ezgif.")

    file_id = file_input["value"]
    form = soup.find("form", class_="ajax-form")
    action_url = form["action"] if form and form.get("action") else f"{EZGIF_COMPRESS_URL}/{file_id}"

    logger.info(f"ezgif Step 1 OK. File ID: {file_id}")
    return {"file_id": file_id, "action_url": action_url, "session": session}


def ezgif_step2_compress(file_id, action_url, session, resolution="original", bitrate=500, output_format="mp4"):
    data = {
        "file": file_id,
        "resolution": resolution,
        "bitrate": str(bitrate),
        "format": output_format,
        "video-compressor": "Recompress video!",
    }
    logger.info(f"ezgif Step 2: Compressing {file_id} (bitrate={bitrate}, res={resolution})")
    resp = session.post(action_url, data=data, timeout=300)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    output_div = soup.find("div", id="output")
    if not output_div:
        error = soup.find("p", class_="error")
        if error:
            raise Exception(f"ezgif compression error: {error.get_text(strip=True)}")
        raise Exception("Сжатие не удалось.")

    video_src = None
    video_tag = output_div.find("video")
    if video_tag:
        source = video_tag.find("source")
        if source and source.get("src"):
            video_src = source["src"]
            if video_src.startswith("//"):
                video_src = "https:" + video_src
            elif video_src.startswith("/"):
                video_src = EZGIF_BASE + video_src

    save_link = output_div.find("a", class_="save")
    if not save_link:
        save_links = soup.find_all("a", class_="save")
        if save_links:
            save_link = save_links[-1]

    if save_link and save_link.get("href"):
        save_url = save_link["href"]
        if save_url.startswith("/"):
            save_url = EZGIF_BASE + save_url
    elif video_src:
        save_url = video_src
    else:
        raise Exception("Не удалось найти URL сжатого видео.")

    filestats = output_div.find("p", class_="filestats")
    if filestats:
        raw = filestats.get_text(" ", strip=True)
        cut = raw.lower().find("convert")
        file_info = raw[:cut].strip() if cut > 0 else raw[:150]
        if not file_info:
            file_info = raw[:150] if raw else "Unknown"
    else:
        file_info = "Unknown"

    logger.info(f"ezgif Step 2 OK. Save URL: {save_url}")
    return {"save_url": save_url, "video_src": video_src, "file_info": file_info, "session": session}


def ezgif_step3_download(save_url, session):
    logger.info(f"ezgif Step 3: Downloading from {save_url}")
    resp = session.get(save_url, timeout=300, stream=True)
    resp.raise_for_status()

    ext = ".mp4"
    for fmt in [".webm", ".mkv", ".avi", ".mov"]:
        if fmt[1:] in save_url:
            ext = fmt
            break

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
    for chunk in resp.iter_content(chunk_size=8192):
        tmp.write(chunk)
    tmp.close()

    file_size = os.path.getsize(tmp.name)
    logger.info(f"ezgif Step 3 OK. File: {tmp.name}, Size: {file_size} bytes")
    return tmp.name


def ezgif_step4_upload_tempshare(file_path, duration=TEMPSHARE_EZGIF_DURATION):
    logger.info(f"ezgif Step 4: Uploading to tempshare.su")
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        data = {"duration": str(duration)}
        resp = requests.post(TEMPSHARE_UPLOAD_URL, files=files, data=data, timeout=300)

    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        raise Exception(f"tempshare upload failed: {result}")

    logger.info(f"ezgif Step 4 OK. raw_url: {result.get('raw_url')}")
    return result


# ===================== UPLOAD TO TEMPSHARE (async) =====================

async def upload_to_tempshare(file_path: str, duration: int = 3) -> dict:
    url = "https://api.tempshare.su/upload"
    timeout = aiohttp.ClientTimeout(total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        with open(file_path, 'rb') as f:
            form = aiohttp.FormData()
            form.add_field('file', f, filename=os.path.basename(file_path))
            form.add_field('duration', str(duration))
            
            async with session.post(url, data=form) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    text = await response.text()
                    raise RuntimeError(f"Upload failed: {response.status} - {text}")


# ===================== HANDLERS =====================

def get_main_keyboard():
    keyboard = [
        [InlineKeyboardButton(text="🎬 Склеить видео", callback_data="menu_merge")],
        [InlineKeyboardButton(text="📥 Скачать видео", callback_data="menu_download")],
        [InlineKeyboardButton(text="📦 Сжать видео", callback_data="menu_compress")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="menu_help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    cleanup_merge_data(user_id)
    
    welcome_text = (
        "<b>🐙 Tentacles Bot</b>\n\n"
        "Многофункциональный бот для работы с видео:\n\n"
        "<b>🎬 Склейка</b> — объединение видео с водяным знаком\n"
        "<b>📥 Скачивание</b> — загрузка видео с 1000+ сайтов\n"
        "<b>📦 Сжатие</b> — уменьшение размера до нужного\n\n"
        "Выберите действие:"
    )
    await message.answer(welcome_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())


@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "<b>🐙 Tentacles Bot — Справка</b>\n\n"
        "<b>Основные команды:</b>\n"
        "/start — Главное меню\n"
        "/merge — Склейка видео\n"
        "/download — Скачать видео по ссылке\n"
        "/compress — Сжать видео (выбор варианта)\n"
        "/help — Эта справка\n\n"

        "<b>🎬 Склейка видео:</b>\n"
        "Отправьте несколько видео, затем /merge_now\n"
        "/watermark — добавить водяной знак (изображение)\n"
        "/size &lt;число&gt; — размер водяного знака в % от высоты видео\n"
        "  Допустимые значения: от 1 до 50 (по умолчанию 15)\n"
        "  Пример: <code>/size 20</code> — знак займёт 20% высоты\n"
        "/speed &lt;число&gt; — скорость движения водяного знака (px/сек)\n"
        "  Допустимые значения: от 10 до 200 (по умолчанию 50)\n"
        "  Чем выше значение, тем быстрее знак перемещается по экрану\n"
        "  Пример: <code>/speed 80</code> — быстрое движение\n"
        "/merge_clear — очистить очередь видео\n"
        "/merge_status — статус очереди\n\n"

        "<b>📥 Скачивание:</b>\n"
        "Поддерживается 1000+ сайтов: TikTok, Instagram, Twitter, VK и др.\n\n"

        "<b>📦 Сжатие (2 варианта):</b>\n\n"
        "<b>Вариант 1 — FFmpeg (локальное):</b>\n"
        "Указываете целевой размер (МБ) и кодек.\n"
        "Кодеки: H.264 (быстро), H.265 (лучше), AV1 (лучшее сжатие, на 30-50%)\n"
        "Быстрая команда: <code>/compress ссылка размер_мб [h264|h265|av1]</code>\n\n"
        "<b>Вариант 2 — ezgif.com (облачное):</b>\n"
        "Сжатие через сервис ezgif.com.\n"
        "Выбираете разрешение, битрейт и формат (MP4/WebM/MKV).\n"
        "Файлы любого размера (крупные авто-разбиваются на части по 190 МБ).\n"
        "Ссылка хранится 7 дней.\n\n"

        "<b>Общее:</b>\n"
        "/status — статус текущей задачи сжатия\n"
        "/cancel — отмена текущей операции"
    )
    await message.answer(help_text, parse_mode=ParseMode.HTML)


@router.callback_query(F.data == "menu_help")
async def callback_help(callback: CallbackQuery):
    await callback.answer()
    help_text = (
        "<b>🐙 Tentacles Bot — Справка</b>\n\n"
        "<b>Команды:</b>\n"
        "/start — Главное меню\n"
        "/merge — Склейка видео\n"
        "/download — Скачать видео по ссылке\n"
        "/compress — Сжать видео\n\n"

        "<b>🎬 Склейка:</b> Отправьте видео -> /merge_now\n"
        "/size &lt;1-50&gt; — размер водяного знака (% высоты видео)\n"
        "/speed &lt;10-200&gt; — скорость движения знака (px/сек)\n\n"

        "<b>📥 Скачивание:</b> 1000+ сайтов\n\n"

        "<b>📦 Сжатие:</b>\n"
        "Вариант 1: FFmpeg — H.264, H.265, AV1\n"
        "Вариант 2: ezgif — разрешение, битрейт, формат (любой размер)"
    )
    keyboard = [[InlineKeyboardButton(text="← Назад", callback_data="back_to_main")]]
    await callback.message.edit_text(help_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data == "back_to_main")
async def callback_back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    welcome_text = (
        "<b>🐙 Tentacles Bot</b>\n\n"
        "Выберите действие:"
    )
    await callback.message.edit_text(welcome_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())


# ===================== MERGE HANDLERS =====================

@router.message(Command("merge"))
@router.callback_query(F.data == "menu_merge")
async def cmd_merge(update: Message | CallbackQuery, state: FSMContext):
    if isinstance(update, CallbackQuery):
        await update.answer()
        message = update.message
    else:
        message = update
    
    user_id = update.from_user.id
    get_merge_data(user_id)
    await state.set_state(MergeStates.collecting_videos)
    
    text = (
        "<b>🎬 Склейка видео</b>\n\n"
        "Отправляйте мне видео файлы, и я объединю их в одно!\n\n"
        "<b>Команды:</b>\n"
        "/merge_now — Склеить все видео\n"
        "/watermark — Установить водяной знак\n"
        "/size &lt;число&gt; — Размер знака (1-50%, по умолчанию 15)\n"
        "/speed &lt;число&gt; — Скорость движения (10-200 px/сек, по умолчанию 50)\n"
        "/merge_status — Статус очереди\n"
        "/merge_clear — Очистить очередь\n\n"
        "Отправьте видео для начала:"
    )
    keyboard = [[InlineKeyboardButton(text="← Назад", callback_data="back_to_main")]]
    
    if isinstance(update, CallbackQuery):
        await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.message(Command("merge_clear"))
async def cmd_merge_clear(message: Message):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    had_watermark = data.watermark_path is not None
    clear_merge_videos(user_id)
    wm_note = "\nВодяной знак сохранён." if had_watermark else ""
    await message.answer(f"✅ Все видео удалены.{wm_note}")


@router.message(Command("merge_status"))
async def cmd_merge_status(message: Message):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    video_count = len(data.videos)
    wm_status = "✅ Установлен" if data.watermark_path else "❌ Нет"
    
    status_text = (
        f"<b>📊 Статус склейки:</b>\n\n"
        f"Видео в очереди: <b>{video_count}</b>\n"
        f"Водяной знак: {wm_status}\n"
        f"Размер знака: <b>{data.watermark_size}%</b>\n"
        f"Скорость: <b>{data.watermark_speed}</b> px/сек"
    )
    await message.answer(status_text, parse_mode=ParseMode.HTML)


@router.message(Command("watermark"))
async def cmd_watermark(message: Message, state: FSMContext):
    await state.set_state(MergeStates.waiting_for_watermark)
    await message.answer(
        "Отправьте изображение для водяного знака\n"
        "(как фото или файл)\n\n"
        "/cancel — отмена"
    )


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    user_id = message.from_user.id
    # Cancel running compression task if any
    task = active_compress_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()
    active_compress_jobs.pop(user_id, None)
    await state.clear()
    await message.answer("❌ Операция отменена", reply_markup=get_main_keyboard())


@router.message(MergeStates.waiting_for_watermark, F.document)
async def process_watermark_document(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    document = message.document
    if not document.mime_type or not document.mime_type.startswith('image/'):
        await message.answer("❌ Отправьте изображение!")
        return
    
    file = await bot.get_file(document.file_id)
    file_ext = Path(document.file_name).suffix if document.file_name else '.png'
    watermark_path = os.path.join(data.temp_dir, f"watermark{file_ext}")
    
    await bot.download_file(file.file_path, watermark_path)
    data.watermark_path = watermark_path
    
    await state.set_state(MergeStates.collecting_videos)
    await message.answer(f"✅ Водяной знак установлен!\nРазмер: {data.watermark_size}%")


@router.message(MergeStates.waiting_for_watermark, F.photo)
async def process_watermark_photo(message: Message, state: FSMContext):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    photo = message.photo[-1]
    file = await bot.get_file(photo.file_id)
    watermark_path = os.path.join(data.temp_dir, "watermark.jpg")
    
    await bot.download_file(file.file_path, watermark_path)
    data.watermark_path = watermark_path
    
    await state.set_state(MergeStates.collecting_videos)
    await message.answer(f"✅ Водяной знак установлен!\nРазмер: {data.watermark_size}%")


@router.message(Command("size"))
async def cmd_size(message: Message):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "Укажите размер водяного знака в % от высоты видео.\n"
                "Допустимые значения: от 1 до 50 (по умолчанию 15)\n\n"
                "Пример: <code>/size 20</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        size = int(parts[1])
        if not 1 <= size <= 50:
            await message.answer("Размер: от 1 до 50%")
            return
        
        data.watermark_size = size
        await message.answer(f"✅ Размер водяного знака: <b>{size}%</b>", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.answer("Введите целое число. Пример: <code>/size 15</code>", parse_mode=ParseMode.HTML)


@router.message(Command("speed"))
async def cmd_speed(message: Message):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "Укажите скорость движения водяного знака в px/сек.\n"
                "Допустимые значения: от 10 до 200 (по умолчанию 50)\n"
                "Чем выше значение, тем быстрее знак перемещается.\n\n"
                "Пример: <code>/speed 80</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        speed = float(parts[1])
        if not 10 <= speed <= 200:
            await message.answer("Скорость: от 10 до 200 px/сек")
            return
        
        data.watermark_speed = speed
        await message.answer(f"✅ Скорость: <b>{speed}</b> px/сек", parse_mode=ParseMode.HTML)
    except ValueError:
        await message.answer("Введите число. Пример: <code>/speed 50</code>", parse_mode=ParseMode.HTML)


@router.message(Command("merge_now"))
async def cmd_merge_now(message: Message):
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    if len(data.videos) < 1:
        await message.answer("❌ Нет видео!\nОтправьте минимум 1 видео.")
        return
    
    if data.watermark_path and not os.path.exists(data.watermark_path):
        data.watermark_path = None
    
    status_msg = await message.answer("⏳ Начинаю обработку...")
    
    async def update_status(text: str):
        try:
            await status_msg.edit_text(f"⏳ {text}")
        except:
            pass
    
    try:
        merged_path = await merge_videos_process(data, update_status)
        
        file_size = os.path.getsize(merged_path)
        file_size_mb = file_size / (1024 * 1024)
        
        await update_status(f"Загрузка ({file_size_mb:.1f} MB)...")
        
        result = await upload_to_tempshare(merged_path, duration=3)
        
        if result.get('success'):
            download_url = result.get('url', '')
            raw_url = result.get('raw_url', '')
            expires = format_expires(result.get('expires', ''))
            
            wm_status = "✅ Добавлен" if data.watermark_path else "❌ Нет"
            
            success_text = (
                f"<b>✅ Видео готово!</b>\n\n"
                f"Склеено: <b>{len(data.videos)}</b> файлов\n"
                f"Размер: <b>{file_size_mb:.1f} MB</b>\n"
                f"Водяной знак: {wm_status}\n\n"
                f"<b>Ссылка:</b>\n{download_url}\n\n"
                f"<b>Прямая ссылка:</b>\n{raw_url}\n\n"
                f"Действует до: {expires}\n\n"
                f"/merge_clear — начать заново"
            )
            await status_msg.edit_text(success_text, parse_mode=ParseMode.HTML)
        else:
            await status_msg.edit_text(f"❌ Ошибка загрузки: {result}")
            
    except Exception as e:
        logger.exception("Error during merge")
        await status_msg.edit_text(f"❌ Ошибка: {str(e)}")


async def is_valid_video(file_path: str) -> bool:
    """Check video integrity via ffprobe — catches moov atom missing, corrupted files, etc."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_type',
        '-of', 'csv=p=0', file_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    return proc.returncode == 0 and b'video' in stdout


@router.message(F.video)
async def handle_video(message: Message, state: FSMContext):
    current_state = await state.get_state()
    
    # Accept videos when in merge mode or no state
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    video = message.video
    
    # Check file size - Telegram Bot API limit is 20MB for downloading
    file_size_mb = (video.file_size or 0) / (1024 * 1024)
    if file_size_mb > 20:
        await message.answer(
            f"⚠️ Видео слишком большое ({file_size_mb:.1f} МБ).\n\n"
            f"Telegram Bot API позволяет скачивать файлы до 20 МБ.\n\n"
            f"<b>Решения:</b>\n"
            f"1. Отправьте видео как <b>файл</b> (документ) меньше 20 МБ\n"
            f"2. Сожмите видео перед отправкой\n"
            f"3. Используйте /compress для сжатия по ссылке",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        file = await bot.get_file(video.file_id)
        
        video_filename = f"video_{len(data.videos) + 1}_{uuid.uuid4().hex[:8]}.mp4"
        video_path = os.path.join(data.temp_dir, video_filename)
        
        await bot.download_file(file.file_path, video_path)

        if not await is_valid_video(video_path):
            os.remove(video_path)
            await message.answer(
                "⚠️ Видео повреждено или скачалось неполностью.\n"
                "Попробуйте отправить ещё раз."
            )
            return

        data.videos.append(video_path)
        
        await message.answer(
            f"✅ Видео #{len(data.videos)} добавлено!\n"
            f"Всего: {len(data.videos)}\n\n"
            f"/merge_now — склеить"
        )
    except Exception as e:
        logger.error(f"Error downloading video: {e}")
        await message.answer(
            f"❌ Не удалось скачать видео.\n"
            f"Попробуйте отправить файл меньшего размера (до 20 МБ)."
        )


@router.message(F.document)
async def handle_document(message: Message, state: FSMContext):
    current_state = await state.get_state()
    
    if current_state == MergeStates.waiting_for_watermark:
        return
    
    user_id = message.from_user.id
    data = get_merge_data(user_id)
    
    document = message.document
    mime_type = document.mime_type or ""
    file_name = document.file_name or ""
    
    video_extensions = ('.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv', '.m4v')
    is_video = mime_type.startswith('video/') or file_name.lower().endswith(video_extensions)
    
    if not is_video:
        if mime_type.startswith('image/'):
            await message.answer("Это изображение. Для водяного знака используйте /watermark")
        return
    
    # Check file size - Telegram Bot API limit is 20MB for downloading
    file_size_mb = (document.file_size or 0) / (1024 * 1024)
    if file_size_mb > 20:
        await message.answer(
            f"⚠️ Файл слишком большой ({file_size_mb:.1f} МБ).\n\n"
            f"Telegram Bot API позволяет скачивать файлы до 20 МБ.\n\n"
            f"<b>Решения:</b>\n"
            f"1. Отправьте видео меньше 20 МБ\n"
            f"2. Сожмите видео перед отправкой\n"
            f"3. Используйте /compress для сжатия по ссылке",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        file = await bot.get_file(document.file_id)
        
        ext = Path(file_name).suffix if file_name else '.mp4'
        video_filename = f"video_{len(data.videos) + 1}_{uuid.uuid4().hex[:8]}{ext}"
        video_path = os.path.join(data.temp_dir, video_filename)
        
        await bot.download_file(file.file_path, video_path)

        if not await is_valid_video(video_path):
            os.remove(video_path)
            await message.answer(
                "⚠️ Файл повреждён или скачался неполностью.\n"
                "Попробуйте отправить ещё раз."
            )
            return

        data.videos.append(video_path)
        
        await message.answer(
            f"✅ Видео #{len(data.videos)} добавлено!\n"
            f"Всего: {len(data.videos)}\n\n"
            f"/merge_now — склеить"
        )
    except Exception as e:
        logger.error(f"Error downloading document: {e}")
        await message.answer(
            f"❌ Не удалось скачать файл.\n"
            f"Попробуйте отправить файл меньшего размера (до 20 МБ)."
        )


# ===================== DOWNLOAD HANDLERS =====================

@router.message(Command("download"))
@router.callback_query(F.data == "menu_download")
async def cmd_download(update: Message | CallbackQuery, state: FSMContext):
    if isinstance(update, CallbackQuery):
        await update.answer()
        message = update.message
    else:
        message = update
    
    await state.set_state(DownloadStates.waiting_for_url)
    
    text = (
        "<b>📥 Скачивание видео</b>\n\n"
        "Отправь мне ссылку на видео, и я скачаю его "
        "и загружу на tempshare.su (хранится 3 дня).\n\n"
        "Поддерживается 1000+ сайтов: TikTok, Instagram, Twitter, VK и др.\n\n"
        "/cancel — отмена"
    )
    keyboard = [[InlineKeyboardButton(text="← Назад", callback_data="back_to_main")]]
    
    if isinstance(update, CallbackQuery):
        await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.message(DownloadStates.waiting_for_url, F.text)
async def handle_download_url(message: Message, state: FSMContext):
    url = message.text.strip()
    
    if url.startswith('/'):
        return
    
    if not is_url(url):
        await message.answer("❌ Отправь ссылку (http:// или https://)")
        return
    
    await state.clear()
    status_msg = await message.answer("⏳ Скачиваю видео...")
    
    tmp_dir = tempfile.mkdtemp(prefix="tentacles_download_")
    try:
        file_path = await download_video_ytdlp(url, tmp_dir)
        
        if not file_path or not os.path.exists(file_path):
            await status_msg.edit_text("❌ Не удалось скачать. Проверь ссылку.")
            return
        
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        await status_msg.edit_text(f"⏳ Скачано ({file_size_mb:.1f} МБ). Загружаю...")
        
        result = await upload_to_tempshare(file_path)
        
        if result and result.get("success"):
            raw_url = result.get("raw_url", "")
            expires = format_expires(result.get("expires", ""))
            await status_msg.edit_text(
                f"<b>✅ Готово!</b>\n\n"
                f"<b>Ссылка:</b>\n<code>{raw_url}</code>\n\n"
                f"<b>Размер:</b> {file_size_mb:.1f} МБ\n"
                f"<b>Действует до:</b> {expires}",
                parse_mode=ParseMode.HTML,
            )
        else:
            await status_msg.edit_text("❌ Не удалось загрузить на tempshare.su")
    except Exception as e:
        logger.error(f"Error: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {str(e)[:200]}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ===================== COMPRESS MENU (выбор варианта) =====================

@router.message(Command("compress"))
@router.callback_query(F.data == "menu_compress")
async def cmd_compress(update: Message | CallbackQuery, state: FSMContext):
    if isinstance(update, CallbackQuery):
        await update.answer()
        message = update.message
    else:
        message = update
    
    user_id = update.from_user.id
    
    if user_id in active_compress_jobs:
        await (message if isinstance(update, Message) else message).answer("⏳ У тебя уже есть активная задача. Дождись завершения.")
        if isinstance(update, CallbackQuery):
            return
        return
    
    # Check for inline arguments (shortcut for variant 1)
    if isinstance(update, Message) and update.text:
        args = update.text.split()[1:]
        if len(args) >= 2:
            url = args[0]
            try:
                target_mb = float(args[1])
                codec = "h264"
                if len(args) >= 3:
                    c = args[2].lower()
                    if c in ("h265", "hevc"):
                        codec = "h265"
                    elif c in ("av1", "svtav1"):
                        codec = "av1"
                
                await state.update_data(compress_url=url, compress_size=target_mb, compress_codec=codec)
                task = asyncio.create_task(_start_compression(message, state, user_id))
                active_compress_tasks[user_id] = task
                return
            except ValueError:
                pass
    
    # Show variant selection menu
    text = (
        "<b>📦 Сжатие видео</b>\n\n"
        "Выбери вариант сжатия:\n\n"
        "<b>Вариант 1 — FFmpeg (локальное)</b>\n"
        "Указываешь целевой размер и кодек (H.264/H.265/AV1).\n"
        "Гарантированный результат по размеру.\n\n"
        "<b>Вариант 2 — ezgif.com (облачное)</b>\n"
        "Выбираешь разрешение, битрейт и формат.\n"
        "Файлы любого размера (авто-разбивка на части).\n\n"
        "/cancel — отмена"
    )
    keyboard = [
        [InlineKeyboardButton(text="🔧 Вариант 1: FFmpeg", callback_data="compress_v1")],
        [InlineKeyboardButton(text="☁️ Вариант 2: ezgif", callback_data="compress_v2")],
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_main")],
    ]
    
    if isinstance(update, CallbackQuery):
        await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    else:
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


# ===================== COMPRESS VARIANT 1: FFmpeg HANDLERS =====================

@router.callback_query(F.data == "compress_v1")
async def compress_v1_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(CompressStates.waiting_for_url)
    
    text = (
        "<b>🔧 Сжатие — Вариант 1 (FFmpeg)</b>\n\n"
        "Отправь <b>прямую ссылку</b> на видео для сжатия.\n\n"
        "Или используй:\n"
        "<code>/compress ссылка размер_мб [h264|h265|av1]</code>\n\n"
        "Пример: <code>/compress https://... 50 av1</code>\n\n"
        "/cancel — отмена"
    )
    keyboard = [[InlineKeyboardButton(text="← Назад", callback_data="menu_compress")]]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.message(CompressStates.waiting_for_url, F.text)
async def receive_compress_url(message: Message, state: FSMContext):
    url = message.text.strip()
    
    if url.startswith('/'):
        return
    
    if not is_url(url):
        await message.answer("❌ Это не ссылка. Отправь HTTP-ссылку на видео.")
        return
    
    await state.update_data(compress_url=url)
    await state.set_state(CompressStates.waiting_for_size)
    
    keyboard = [
        [InlineKeyboardButton(text="25 МБ", callback_data="csize_25"),
         InlineKeyboardButton(text="50 МБ", callback_data="csize_50")],
        [InlineKeyboardButton(text="99 МБ", callback_data="csize_99"),
         InlineKeyboardButton(text="200 МБ", callback_data="csize_200")],
        [InlineKeyboardButton(text="500 МБ", callback_data="csize_500"),
         InlineKeyboardButton(text="Свой размер", callback_data="csize_custom")],
        [InlineKeyboardButton(text="Отмена", callback_data="csize_cancel")],
    ]
    await message.answer("Выбери целевой размер:", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data.startswith("csize_"))
async def compress_size_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = callback.data
    
    if data == "csize_cancel":
        await state.clear()
        await callback.message.edit_text("❌ Отменено")
        return
    
    if data == "csize_custom":
        await state.set_state(CompressStates.waiting_for_custom_size)
        await callback.message.edit_text("Введи размер в МБ (например: 75):")
        return
    
    size_map = {"csize_25": 25, "csize_50": 50, "csize_99": 99, "csize_200": 200, "csize_500": 500}
    target_mb = size_map.get(data, 99)
    await state.update_data(compress_size=target_mb)
    await state.set_state(CompressStates.waiting_for_codec)
    
    keyboard = [
        [InlineKeyboardButton(text="H.264 (быстро)", callback_data="ccodec_h264"),
         InlineKeyboardButton(text="H.265 (лучше)", callback_data="ccodec_h265")],
        [InlineKeyboardButton(text="AV1 (лучшее сжатие)", callback_data="ccodec_av1")],
        [InlineKeyboardButton(text="Отмена", callback_data="ccodec_cancel")],
    ]
    await callback.message.edit_text(
        f"Размер: <b>{target_mb} МБ</b>\nВыбери кодек:\n\n"
        f"<b>AV1</b> — на 30-50% лучшее сжатие",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )


@router.message(CompressStates.waiting_for_custom_size, F.text)
async def receive_custom_compress_size(message: Message, state: FSMContext):
    try:
        target_mb = float(message.text.strip())
        if target_mb <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Введи положительное число. Пример: 75")
        return
    
    await state.update_data(compress_size=target_mb)
    await state.set_state(CompressStates.waiting_for_codec)
    
    keyboard = [
        [InlineKeyboardButton(text="H.264 (быстро)", callback_data="ccodec_h264"),
         InlineKeyboardButton(text="H.265 (лучше)", callback_data="ccodec_h265")],
        [InlineKeyboardButton(text="AV1 (лучшее сжатие)", callback_data="ccodec_av1")],
        [InlineKeyboardButton(text="Отмена", callback_data="ccodec_cancel")],
    ]
    await message.answer(
        f"Размер: <b>{target_mb} МБ</b>\nВыбери кодек:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )


@router.callback_query(F.data.startswith("ccodec_"))
async def compress_codec_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = callback.data
    
    if data == "ccodec_cancel":
        await state.clear()
        await callback.message.edit_text("❌ Отменено")
        return
    
    codec_map = {"ccodec_h264": "h264", "ccodec_h265": "h265", "ccodec_av1": "av1"}
    codec = codec_map.get(data, "h264")
    await state.update_data(compress_codec=codec)
    
    user_id = callback.from_user.id
    state_data = await state.get_data()
    
    codec_names = {"h264": "H.264", "h265": "H.265", "av1": "AV1"}
    
    await callback.message.edit_text(
        f"⏳ Запускаю сжатие...\n"
        f"Цель: {state_data.get('compress_size')} МБ | Кодек: {codec_names.get(codec)}"
    )
    
    task = asyncio.create_task(_start_compression(callback.message, state, user_id))
    active_compress_tasks[user_id] = task


async def _start_compression(message: Message, state: FSMContext, user_id: int):
    state_data = await state.get_data()
    url = state_data.get('compress_url')
    target_mb = state_data.get('compress_size')
    codec = state_data.get('compress_codec', 'h264')
    
    await state.clear()
    
    codec_names = {"h264": "H.264", "h265": "H.265", "av1": "AV1"}
    codec_label = codec_names.get(codec, codec.upper())
    
    if len(active_compress_jobs) >= MAX_CONCURRENT_JOBS:
        await message.answer(f"⏳ Сервер занят. Попробуй позже.")
        return
    
    active_compress_jobs[user_id] = {
        "status": "Запуск...",
        "start_time": time.time(),
        "url": url,
        "target_mb": target_mb,
        "codec": codec,
    }
    
    status_msg = await message.answer(
        f"⏳ Запускаю сжатие...\nЦель: {target_mb} МБ | Кодек: {codec_label}"
    )
    
    tmpdir = tempfile.mkdtemp(prefix="tentacles_compress_")
    try:
        async with _job_semaphore:
            # Download
            active_compress_jobs[user_id]["status"] = "Скачиваю..."
            await status_msg.edit_text(f"⏳ Скачиваю видео...\nСсылка: {str(url)[:60]}...")
            
            input_path = os.path.join(tmpdir, "input_video")
            max_download_bytes = MAX_DOWNLOAD_SIZE_MB * 1024 * 1024
            
            try:
                async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as http:
                    async with http.stream("GET", url) as response:
                        if response.status_code != 200:
                            await status_msg.edit_text(f"❌ HTTP: {response.status_code}")
                            return
                        
                        total_size = int(response.headers.get("content-length", 0))
                        if total_size > max_download_bytes:
                            await status_msg.edit_text(f"❌ Файл > {MAX_DOWNLOAD_SIZE_MB} МБ")
                            return
                        
                        downloaded = 0
                        last_update = 0
                        
                        with open(input_path, "wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE):
                                f.write(chunk)
                                downloaded += len(chunk)
                                
                                if downloaded > max_download_bytes:
                                    await status_msg.edit_text(f"❌ Превышен лимит {MAX_DOWNLOAD_SIZE_MB} МБ")
                                    return
                                
                                now = time.time()
                                if now - last_update > 3 and total_size > 0:
                                    pct = (downloaded / total_size) * 100
                                    await status_msg.edit_text(f"⏳ Скачиваю: {pct:.0f}%\n{format_size(downloaded)} / {format_size(total_size)}")
                                    last_update = now
            except Exception as e:
                await status_msg.edit_text(f"❌ Ошибка скачивания: {str(e)[:200]}")
                return
            
            # Analyze
            active_compress_jobs[user_id]["status"] = "Анализирую..."
            await status_msg.edit_text("⏳ Анализирую видео...")
            
            try:
                info = await get_video_info(input_path)
            except Exception as e:
                await status_msg.edit_text(f"❌ Не удалось проанализировать: {str(e)[:200]}")
                return
            
            input_size = os.path.getsize(input_path)
            info_text = (
                f"<b>📊 Инфо:</b>\n"
                f"Размер: {format_size(input_size)}\n"
                f"Длительность: {format_duration(info['duration'])}\n"
                f"Разрешение: {info['width']}x{info['height']}\n\n"
                f"⏳ Сжатие до {target_mb} МБ ({codec_label})..."
            )
            await status_msg.edit_text(info_text, parse_mode=ParseMode.HTML)
            
            # Check if already under target
            if input_size <= target_mb * 1024 * 1024:
                active_compress_jobs[user_id]["status"] = "Загружаю..."
                result = await upload_to_tempshare(input_path)
                raw_url = result.get("raw_url", result.get("url", "N/A"))
                expires = format_expires(result.get('expires', ''))
                await status_msg.edit_text(
                    f"✅ Видео уже меньше {target_mb} МБ!\n\n"
                    f"<b>Размер:</b> {format_size(input_size)}\n"
                    f"<b>Ссылка:</b>\n<code>{raw_url}</code>\n\n"
                    f"Действует до: {expires}",
                    parse_mode=ParseMode.HTML,
                )
                return
            
            # Compress
            active_compress_jobs[user_id]["status"] = "Сжимаю..."
            output_path = os.path.join(tmpdir, "compressed.mp4")
            
            last_progress = [0.0]
            
            async def progress_cb(msg):
                active_compress_jobs[user_id]["status"] = msg
                now = time.time()
                if now - last_progress[0] > 4:
                    last_progress[0] = now
                    try:
                        await status_msg.edit_text(f"{info_text}\n\n{msg}", parse_mode=ParseMode.HTML)
                    except:
                        pass
            
            try:
                result = await compress_video(
                    input_path=input_path,
                    output_path=output_path,
                    target_mb=target_mb,
                    codec=codec,
                    progress_callback=progress_cb,
                )
            except Exception as e:
                await status_msg.edit_text(f"❌ Ошибка сжатия: {str(e)[:300]}")
                return
            
            try:
                os.remove(input_path)
            except:
                pass
            
            output_size = result["output_size"]
            compression_ratio = (1 - output_size / result["input_size"]) * 100
            
            # Upload
            active_compress_jobs[user_id]["status"] = "Загружаю..."
            await status_msg.edit_text("⏳ Загружаю на TempShare...")
            
            try:
                upload_result = await upload_to_tempshare(output_path)
            except Exception as e:
                await status_msg.edit_text(f"❌ Ошибка загрузки: {str(e)[:300]}")
                return
            
            raw_url = upload_result.get("raw_url", upload_result.get("url", "N/A"))
            expires = format_expires(upload_result.get('expires', ''))
            elapsed = time.time() - active_compress_jobs[user_id]["start_time"]
            
            final_text = (
                f"<b>✅ Готово! (FFmpeg)</b>\n\n"
                f"Оригинал: {format_size(result['input_size'])}\n"
                f"Сжато: {format_size(output_size)}\n"
                f"Цель: {target_mb} МБ\n"
                f"Сжатие: {compression_ratio:.1f}%\n"
                f"Кодек: {codec_label}\n"
                f"Время: {format_duration(elapsed)}\n\n"
                f"<b>Ссылка:</b>\n<code>{raw_url}</code>\n\n"
                f"Действует до: {expires}"
            )
            
            keyboard = [[InlineKeyboardButton(text="📦 Сжать ещё", callback_data="menu_compress")]]
            await status_msg.edit_text(final_text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
            
    except Exception as e:
        logger.exception("Compress error")
        try:
            await status_msg.edit_text(f"❌ Ошибка: {str(e)[:300]}")
        except:
            pass
    finally:
        active_compress_jobs.pop(user_id, None)
        active_compress_tasks.pop(user_id, None)
        shutil.rmtree(tmpdir, ignore_errors=True)


# ===================== COMPRESS VARIANT 2: ezgif HANDLERS =====================

def ezgif_resolution_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, label in RESOLUTIONS.items():
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"ezres:{key}")])
    buttons.append([InlineKeyboardButton(text="← Назад", callback_data="menu_compress")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def ezgif_bitrate_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, label in BITRATES.items():
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"ezbr:{key}")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="ezcancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def ezgif_format_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, label in EZGIF_FORMATS.items():
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"ezfmt:{key}")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="ezcancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data == "compress_v2")
async def compress_v2_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(EzgifStates.waiting_for_url)
    
    text = (
        "<b>☁️ Сжатие — Вариант 2 (ezgif.com)</b>\n\n"
        "Отправь <b>прямую ссылку</b> на видео.\n\n"
        "Поддерживаемые форматы: MP4, WebM, AVI, MKV, MOV\n"
        "Файлы любого размера (авто-разбивка на части > 190 МБ)\n\n"
        "/cancel — отмена"
    )
    keyboard = [[InlineKeyboardButton(text="← Назад", callback_data="menu_compress")]]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.message(EzgifStates.waiting_for_url, F.text)
async def ezgif_receive_url(message: Message, state: FSMContext):
    url = message.text.strip()
    
    if url.startswith('/'):
        return
    
    if not is_url(url):
        await message.answer("❌ Отправь ссылку (http:// или https://)")
        return
    
    chat_id = message.chat.id
    ezgif_pending[chat_id] = {"url": url, "resolution": None, "bitrate": None}
    await state.set_state(EzgifStates.waiting_for_resolution)
    
    await message.answer(
        "Выбери <b>разрешение</b> для сжатия:",
        parse_mode=ParseMode.HTML,
        reply_markup=ezgif_resolution_keyboard(),
    )


@router.callback_query(F.data == "ezcancel")
async def ezgif_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    chat_id = callback.message.chat.id
    ezgif_pending.pop(chat_id, None)
    await state.clear()
    await callback.message.edit_text("❌ Отменено", reply_markup=None)


@router.callback_query(F.data.startswith("ezres:"))
async def ezgif_on_resolution(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    resolution = callback.data.split(":", 1)[1]
    
    if chat_id not in ezgif_pending:
        await callback.answer("Сначала отправь ссылку на видео.", show_alert=True)
        return
    
    ezgif_pending[chat_id]["resolution"] = resolution
    res_label = RESOLUTIONS.get(resolution, resolution)
    await state.set_state(EzgifStates.waiting_for_bitrate)
    
    await callback.message.edit_text(
        f"Разрешение: <b>{res_label}</b>\n\n"
        "Теперь выбери <b>битрейт</b>:",
        parse_mode=ParseMode.HTML,
        reply_markup=ezgif_bitrate_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ezbr:"))
async def ezgif_on_bitrate(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    bitrate = callback.data.split(":", 1)[1]
    
    if chat_id not in ezgif_pending:
        await callback.answer("Сначала отправь ссылку на видео.", show_alert=True)
        return
    
    ezgif_pending[chat_id]["bitrate"] = bitrate
    res_label = RESOLUTIONS.get(ezgif_pending[chat_id]["resolution"], ezgif_pending[chat_id]["resolution"])
    br_label = BITRATES.get(bitrate, f"{bitrate} kbps")
    await state.set_state(EzgifStates.waiting_for_format)
    
    await callback.message.edit_text(
        f"Разрешение: <b>{res_label}</b>\n"
        f"Битрейт: <b>{br_label}</b>\n\n"
        "Теперь выбери <b>формат</b>:",
        parse_mode=ParseMode.HTML,
        reply_markup=ezgif_format_keyboard(),
    )
    await callback.answer()


EZGIF_CHUNK_MB = 190  # Split threshold and chunk size (keep under 200MB limit)


async def download_video_for_chunks(url: str, output_path: str, status_msg, settings_text: str) -> int:
    """Download video to local file with progress, return file size."""
    timeout = httpx.Timeout(900.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as http:
        async with http.stream("GET", url) as response:
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            last_update = time.time()
            with open(output_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.time()
                    if now - last_update > 3 and total_size > 0:
                        pct = (downloaded / total_size) * 100
                        try:
                            await status_msg.edit_text(
                                f"{settings_text}\n\n⏳ Скачиваю видео: {pct:.0f}%\n"
                                f"{format_size(downloaded)} / {format_size(total_size)}",
                                parse_mode=ParseMode.HTML,
                            )
                        except Exception:
                            pass
                        last_update = now
    return os.path.getsize(output_path)


async def split_video_into_chunks(input_path: str, chunk_size_mb: float, temp_dir: str) -> List[str]:
    """Split video into chunks of ~chunk_size_mb MB each by time proportion."""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", input_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    info = json.loads(stdout.decode())
    duration = float(info["format"]["duration"])
    file_size = os.path.getsize(input_path)

    num_chunks = math.ceil(file_size / (chunk_size_mb * 1024 * 1024))
    if num_chunks <= 1:
        return [input_path]

    chunk_duration = duration / num_chunks
    chunks = []
    for i in range(num_chunks):
        start = i * chunk_duration
        chunk_path = os.path.join(temp_dir, f"chunk_{i:03d}.mp4")
        cmd = [
            "ffmpeg", "-y", "-ss", str(start), "-i", input_path,
            "-t", str(chunk_duration),
            "-c", "copy", "-avoid_negative_ts", "1",
            "-movflags", "+faststart", chunk_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if os.path.exists(chunk_path) and os.path.getsize(chunk_path) > 0:
            chunks.append(chunk_path)
    return chunks


def _process_chunk_sync(chunk_path: str, resolution: str, bitrate: int, fmt: str) -> str:
    """Upload chunk to tempshare → compress via ezgif → return compressed local path."""
    # Upload chunk to tempshare to get a URL for ezgif
    with open(chunk_path, "rb") as f:
        resp = requests.post(
            TEMPSHARE_UPLOAD_URL,
            files={"file": (os.path.basename(chunk_path), f)},
            data={"duration": "1"},
            timeout=300,
        )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("success"):
        raise RuntimeError(f"Tempshare upload failed: {result}")
    chunk_url = result["raw_url"]

    # Run ezgif pipeline
    upload_result = ezgif_step1_upload_url(chunk_url)
    compress_result = ezgif_step2_compress(
        file_id=upload_result["file_id"],
        action_url=upload_result["action_url"],
        session=upload_result["session"],
        resolution=resolution,
        bitrate=bitrate,
        output_format=fmt,
    )
    compressed_path = ezgif_step3_download(
        save_url=compress_result["save_url"],
        session=compress_result["session"],
    )
    return compressed_path


async def concat_video_chunks(chunk_paths: List[str], output_path: str):
    """Concatenate compressed chunks into one file using ffmpeg."""
    list_file = output_path + ".list.txt"
    with open(list_file, "w") as f:
        for p in chunk_paths:
            f.write(f"file '{p}'\n")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", list_file, "-c", "copy",
        "-movflags", "+faststart", output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    try:
        os.remove(list_file)
    except OSError:
        pass
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg concat failed: {stderr.decode()[:300]}")


@router.callback_query(F.data.startswith("ezfmt:"))
async def ezgif_on_format(callback: CallbackQuery, state: FSMContext):
    chat_id = callback.message.chat.id
    fmt = callback.data.split(":", 1)[1]

    if chat_id not in ezgif_pending:
        await callback.answer("Сначала отправь ссылку на видео.", show_alert=True)
        return

    data = ezgif_pending.pop(chat_id)
    await state.clear()

    video_url = data["url"]
    resolution = data["resolution"] or "original"
    bitrate = int(data["bitrate"] or 500)
    res_label = RESOLUTIONS.get(resolution, resolution)
    br_label = BITRATES.get(str(bitrate), f"{bitrate} kbps")
    fmt_label = EZGIF_FORMATS.get(fmt, fmt.upper())
    settings_text = (
        f"<b>☁️ ezgif.com — Сжатие</b>\n"
        f"Настройки: {res_label} / {br_label} / {fmt_label}"
    )

    status_msg = await callback.message.edit_text(
        f"{settings_text}\n\n⏳ Проверяю размер видео...",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()

    loop = asyncio.get_event_loop()
    start_time = time.monotonic()
    tmpdir = tempfile.mkdtemp(prefix="tentacles_ezgif_")

    try:
        # ── Проверяем размер через HEAD-запрос ──────────────────────────
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as http:
            head = await http.head(video_url)
            content_length = int(head.headers.get("content-length", 0))

        file_size_mb = content_length / (1024 * 1024)
        needs_chunking = file_size_mb > EZGIF_CHUNK_MB

        # ═══════════════════════════════════════════════════════════════
        # РЕЖИМ БЕЗ ЧАНКОВ: файл <= 190 МБ — стандартный поток
        # ═══════════════════════════════════════════════════════════════
        if not needs_chunking:
            await status_msg.edit_text(
                f"{settings_text}\n\nШаг 1/4: Загрузка видео на ezgif.com...",
                parse_mode=ParseMode.HTML,
            )

            upload_result = await loop.run_in_executor(ezgif_executor, ezgif_step1_upload_url, video_url)
            t1 = time.monotonic() - start_time
            await status_msg.edit_text(
                f"{settings_text}\n\nШаг 2/4: Сжатие видео...\n(Шаг 1 занял {t1:.1f}с)",
                parse_mode=ParseMode.HTML,
            )

            compress_result = await loop.run_in_executor(
                ezgif_executor,
                lambda: ezgif_step2_compress(
                    file_id=upload_result["file_id"],
                    action_url=upload_result["action_url"],
                    session=upload_result["session"],
                    resolution=resolution,
                    bitrate=bitrate,
                    output_format=fmt,
                ),
            )
            t2 = time.monotonic() - start_time
            await status_msg.edit_text(
                f"{settings_text}\n\nШаг 3/4: Скачивание сжатого видео...\n"
                f"{compress_result['file_info'][:200]}\n(Сжатие: {t2 - t1:.1f}с)",
                parse_mode=ParseMode.HTML,
            )

            tmp_path = await loop.run_in_executor(
                ezgif_executor,
                lambda: ezgif_step3_download(
                    save_url=compress_result["save_url"],
                    session=compress_result["session"],
                ),
            )
            t3 = time.monotonic() - start_time
            await status_msg.edit_text(
                f"{settings_text}\n\nШаг 4/4: Загрузка на tempshare.su...\n(Скачивание: {t3 - t2:.1f}с)",
                parse_mode=ParseMode.HTML,
            )

            tempshare_result = await loop.run_in_executor(
                ezgif_executor,
                lambda: ezgif_step4_upload_tempshare(tmp_path),
            )
            total_time = time.monotonic() - start_time

            try:
                os.unlink(tmp_path)
            except Exception:
                pass

            raw_url = tempshare_result.get("raw_url", tempshare_result.get("url", "N/A"))
            expires = tempshare_result.get("expires", "N/A")
            t_str = _fmt_time(total_time)
            keyboard = [[InlineKeyboardButton(text="📦 Сжать ещё", callback_data="menu_compress")]]
            await status_msg.edit_text(
                f"<b>✅ Готово! (ezgif.com)</b>\n\n"
                f"Настройки: {res_label} / {br_label} / {fmt_label}\n"
                f"Инфо: {compress_result['file_info'][:200]}\n\n"
                f"Время: <b>{t_str}</b>\n\n"
                f"<b>Ссылка:</b>\n<code>{raw_url}</code>\n\n"
                f"Действительна до: {expires}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
            )
            return

        # ═══════════════════════════════════════════════════════════════
        # РЕЖИМ ЧАНКОВ: файл > 190 МБ
        # ═══════════════════════════════════════════════════════════════
        num_chunks_est = math.ceil(file_size_mb / EZGIF_CHUNK_MB)
        await status_msg.edit_text(
            f"{settings_text}\n\n"
            f"Видео {file_size_mb:.0f} МБ > {EZGIF_CHUNK_MB} МБ\n"
            f"Буду делить на ~{num_chunks_est} частей и сжимать каждую.\n\n"
            f"⏳ Скачиваю видео...",
            parse_mode=ParseMode.HTML,
        )

        # 1. Скачать видео локально
        input_path = os.path.join(tmpdir, "input_video.mp4")
        await download_video_for_chunks(video_url, input_path, status_msg, settings_text)

        actual_size_mb = os.path.getsize(input_path) / (1024 * 1024)
        await status_msg.edit_text(
            f"{settings_text}\n\n"
            f"✅ Скачано: {actual_size_mb:.1f} МБ\n"
            f"⏳ Нарезаю на части по {EZGIF_CHUNK_MB} МБ...",
            parse_mode=ParseMode.HTML,
        )

        # 2. Нарезать на чанки
        chunks_dir = os.path.join(tmpdir, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)
        chunk_paths = await split_video_into_chunks(input_path, EZGIF_CHUNK_MB, chunks_dir)
        num_chunks = len(chunk_paths)

        await status_msg.edit_text(
            f"{settings_text}\n\n"
            f"✅ Нарезано {num_chunks} частей\n"
            f"⏳ Начинаю сжатие через ezgif.com...",
            parse_mode=ParseMode.HTML,
        )

        # 3. Сжать каждый чанк через ezgif
        compressed_paths = []
        for idx, chunk_path in enumerate(chunk_paths, 1):
            chunk_mb = os.path.getsize(chunk_path) / (1024 * 1024)
            await status_msg.edit_text(
                f"{settings_text}\n\n"
                f"Часть {idx}/{num_chunks} ({chunk_mb:.1f} МБ)\n"
                f"⏳ Загружаю на ezgif.com...",
                parse_mode=ParseMode.HTML,
            )

            compressed_path = await loop.run_in_executor(
                ezgif_executor,
                lambda cp=chunk_path: _process_chunk_sync(cp, resolution, bitrate, fmt),
            )
            compressed_paths.append(compressed_path)

            comp_mb = os.path.getsize(compressed_path) / (1024 * 1024)
            elapsed = time.monotonic() - start_time
            await status_msg.edit_text(
                f"{settings_text}\n\n"
                f"✅ Часть {idx}/{num_chunks} сжата → {comp_mb:.1f} МБ\n"
                f"Прошло: {_fmt_time(elapsed)}",
                parse_mode=ParseMode.HTML,
            )

        # 4. Склеить все сжатые чанки
        await status_msg.edit_text(
            f"{settings_text}\n\n"
            f"✅ Все {num_chunks} частей сжаты\n"
            f"⏳ Склеиваю в одно видео...",
            parse_mode=ParseMode.HTML,
        )

        final_path = os.path.join(tmpdir, f"final.{fmt}")
        if len(compressed_paths) == 1:
            final_path = compressed_paths[0]
        else:
            await concat_video_chunks(compressed_paths, final_path)

        final_mb = os.path.getsize(final_path) / (1024 * 1024)
        await status_msg.edit_text(
            f"{settings_text}\n\n"
            f"✅ Готово: {final_mb:.1f} МБ\n"
            f"⏳ Загружаю на tempshare.su...",
            parse_mode=ParseMode.HTML,
        )

        # 5. Загрузить на tempshare
        tempshare_result = await loop.run_in_executor(
            ezgif_executor,
            lambda: ezgif_step4_upload_tempshare(final_path),
        )
        total_time = time.monotonic() - start_time

        raw_url = tempshare_result.get("raw_url", tempshare_result.get("url", "N/A"))
        expires = tempshare_result.get("expires", "N/A")
        keyboard = [[InlineKeyboardButton(text="📦 Сжать ещё", callback_data="menu_compress")]]
        await status_msg.edit_text(
            f"<b>✅ Готово! (ezgif.com, {num_chunks} частей)</b>\n\n"
            f"Настройки: {res_label} / {br_label} / {fmt_label}\n"
            f"Оригинал: {actual_size_mb:.1f} МБ → Результат: {final_mb:.1f} МБ\n"
            f"Частей: {num_chunks}\n"
            f"Время: <b>{_fmt_time(total_time)}</b>\n\n"
            f"<b>Ссылка:</b>\n<code>{raw_url}</code>\n\n"
            f"Действительна до: {expires}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        )

    except Exception as e:
        total_time = time.monotonic() - start_time
        logger.error(f"ezgif error: {e}", exc_info=True)
        keyboard = [[InlineKeyboardButton(text="📦 Попробовать снова", callback_data="menu_compress")]]
        await status_msg.edit_text(
            f"❌ Ошибка при обработке через ezgif (через {total_time:.1f}с):\n<code>{e}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ===================== STATUS =====================

@router.message(Command("status"))
async def cmd_status(message: Message):
    user_id = message.from_user.id
    job = active_compress_jobs.get(user_id)
    if not job:
        await message.answer("Нет активных задач сжатия.")
        return
    
    elapsed = time.time() - job["start_time"]
    await message.answer(
        f"<b>📊 Активная задача</b>\n\n"
        f"Статус: {job['status']}\n"
        f"Время: {format_duration(elapsed)}\n"
        f"Цель: {job.get('target_mb')} МБ\n"
        f"Кодек: {job.get('codec', 'h264').upper()}",
        parse_mode=ParseMode.HTML
    )


# ===================== FALLBACK =====================

@router.message(F.text)
async def handle_text(message: Message, state: FSMContext):
    current_state = await state.get_state()
    
    # If in download state and got URL
    if current_state == DownloadStates.waiting_for_url:
        return
    
    # If in compress states
    if current_state and current_state.startswith("CompressStates"):
        return
    
    # If in ezgif states
    if current_state and current_state.startswith("EzgifStates"):
        return
    
    # Check if it's a URL - auto-download
    url = message.text.strip()
    if is_url(url):
        await state.set_state(DownloadStates.waiting_for_url)
        # Reprocess
        await handle_download_url(message, state)
        return
    
    await message.answer(
        "Не понимаю. Используй /start для главного меню.",
        reply_markup=get_main_keyboard()
    )


# ===================== MAIN =====================

async def main():
    logger.info("Starting Tentacles Bot...")
    
    dp.include_router(router)
    
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
