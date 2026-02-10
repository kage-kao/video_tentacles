import asyncio
import json
import logging
import math
import os
import re
import shutil
import tempfile

logger = logging.getLogger(__name__)

# Memory safeguards
MAX_PARALLEL_SEGMENTS = 1
STDERR_BUFFER_LIMIT = 4096
MAX_FFMPEG_THREADS = 2


async def get_video_info(filepath: str) -> dict:
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", filepath,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {stderr.decode()}")
    info = json.loads(stdout.decode())

    duration = float(info["format"]["duration"])
    size_bytes = int(info["format"].get("size", 0))

    width = height = 0
    codec = "unknown"
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "video":
            width = int(stream.get("width", 0))
            height = int(stream.get("height", 0))
            codec = stream.get("codec_name", "unknown")
            break

    return {
        "duration": duration,
        "size_bytes": size_bytes,
        "width": width,
        "height": height,
        "codec": codec,
    }


async def get_video_duration(filepath: str) -> float:
    info = await get_video_info(filepath)
    return info["duration"]


async def _get_keyframe_times(filepath: str) -> list:
    cmd = [
        "ffprobe", "-v", "quiet", "-select_streams", "v:0",
        "-show_entries", "packet=pts_time,flags",
        "-of", "csv=print_section=0", filepath,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    keyframes = []
    for line in stdout.decode().splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 2 and "K" in parts[1]:
            try:
                keyframes.append(float(parts[0]))
            except (ValueError, IndexError):
                pass
    return sorted(keyframes)


async def _split_video_segments(
    input_path: str, segment_dir: str, num_segments: int, duration: float
) -> list:
    keyframes = await _get_keyframe_times(input_path)

    if len(keyframes) < num_segments * 2:
        return [{"path": input_path, "start": 0, "end": duration, "index": 0}]

    segment_duration = duration / num_segments
    split_points = [0.0]
    for i in range(1, num_segments):
        ideal = i * segment_duration
        closest = min(keyframes, key=lambda kf: abs(kf - ideal))
        if closest > split_points[-1] + 1.0:
            split_points.append(closest)
    split_points.append(duration)
    split_points = sorted(set(split_points))

    segments = []
    for idx in range(len(split_points) - 1):
        start = split_points[idx]
        end = split_points[idx + 1]
        seg_path = os.path.join(segment_dir, f"seg_{idx:03d}.mkv")
        segments.append({"path": seg_path, "start": start, "end": end, "index": idx})

        cmd = [
            "ffmpeg", "-y", "-ss", str(start), "-i", input_path,
            "-t", str(end - start),
            "-c", "copy", "-avoid_negative_ts", "1",
            seg_path,
        ]
        await _run_cmd(cmd)

    valid = [s for s in segments if os.path.exists(s["path"]) and os.path.getsize(s["path"]) > 0]
    if not valid:
        return [{"path": input_path, "start": 0, "end": duration, "index": 0}]
    return valid


async def _run_cmd(cmd: list) -> int:
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.warning(f"Command failed: {' '.join(cmd[:5])}... stderr: {stderr.decode()[:200]}")
    return proc.returncode


async def _encode_segment(
    input_path: str,
    output_path: str,
    video_bitrate: int,
    audio_bitrate: int,
    codec: str,
    preset: str,
    threads: int,
    duration: float,
    progress_callback=None,
    label: str = "",
) -> int:
    """Single-pass encode a segment with constrained bitrate."""
    threads = min(threads, MAX_FFMPEG_THREADS)
    maxrate = int(video_bitrate * 1.05)
    bufsize = int(video_bitrate * 1.5)

    if codec in ("av1", "svtav1"):
        vcodec = "libsvtav1"
        extra = [
            "-svtav1-params",
            f"mbr={maxrate // 1000}:tbr={video_bitrate // 1000}",
        ]
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-c:v", vcodec,
            "-b:v", str(video_bitrate),
            "-maxrate", str(maxrate),
            "-bufsize", str(bufsize),
            "-preset", preset,
            "-g", "240",
            "-c:a", "aac",
            "-b:a", f"{audio_bitrate // 1000}k",
            "-movflags", "+faststart",
        ] + extra + [output_path]
    elif codec in ("h265", "hevc"):
        vcodec = "libx265"
        extra = ["-x265-params", f"log-level=error:pools={threads}"]
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-c:v", vcodec,
            "-b:v", str(video_bitrate),
            "-maxrate", str(maxrate),
            "-bufsize", str(bufsize),
            "-preset", preset,
            "-threads", str(threads),
            "-c:a", "aac",
            "-b:a", f"{audio_bitrate // 1000}k",
            "-movflags", "+faststart",
        ] + extra + [output_path]
    else:
        vcodec = "libx264"
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-c:v", vcodec,
            "-b:v", str(video_bitrate),
            "-maxrate", str(maxrate),
            "-bufsize", str(bufsize),
            "-preset", preset,
            "-threads", str(threads),
            "-c:a", "aac",
            "-b:a", f"{audio_bitrate // 1000}k",
            "-movflags", "+faststart",
            output_path,
        ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    buffer = b""
    last_pct = -1
    while True:
        chunk = await proc.stderr.read(4096)
        if not chunk:
            break
        buffer += chunk
        if len(buffer) > STDERR_BUFFER_LIMIT:
            buffer = buffer[-STDERR_BUFFER_LIMIT:]
        lines = buffer.split(b"\r")
        buffer = lines[-1]
        for line in lines[:-1]:
            text = line.decode(errors="ignore")
            m = re.search(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)", text)
            if m and duration > 0 and progress_callback:
                cur = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
                pct = min(int((cur / duration) * 100), 99)
                if pct != last_pct and pct % 5 == 0:
                    last_pct = pct
                    try:
                        await progress_callback(f"{label}: {pct}%")
                    except Exception:
                        pass

    await proc.wait()
    return proc.returncode


async def _concat_segments(segment_paths: list, output_path: str) -> int:
    list_file = output_path + ".list.txt"
    with open(list_file, "w") as f:
        for p in segment_paths:
            f.write(f"file '{p}'\n")

    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", list_file, "-c", "copy",
        "-movflags", "+faststart",
        output_path,
    ]
    rc = await _run_cmd(cmd)
    try:
        os.remove(list_file)
    except OSError:
        pass
    return rc


async def compress_video(
    input_path: str,
    output_path: str,
    target_mb: float,
    codec: str = "h264",
    progress_callback=None,
) -> dict:
    """
    TURBO compress video to target size with GUARANTEED size constraint.
    """
    duration = await get_video_duration(input_path)
    if duration <= 0:
        raise ValueError("Video duration is 0 or negative")

    input_size = os.path.getsize(input_path)
    target_bytes = int(target_mb * 1024 * 1024)

    if input_size <= target_bytes:
        shutil.copy2(input_path, output_path)
        return {
            "success": True,
            "input_size": input_size,
            "output_size": input_size,
            "target_size": target_bytes,
            "duration": duration,
            "codec": "original",
            "attempts": 1,
            "segments_used": 1,
        }

    safe_target_bytes = int(target_bytes * 0.90)

    audio_bitrate = 128_000
    if target_mb < 10:
        audio_bitrate = 64_000
    elif target_mb < 30:
        audio_bitrate = 96_000

    total_bitrate = (safe_target_bytes * 8) / duration
    video_bitrate = int(total_bitrate - audio_bitrate)

    if video_bitrate < 50_000:
        raise ValueError(
            f"Target {target_mb} MB too small for {duration:.0f}s video. "
            f"Min recommended: {((audio_bitrate + 100_000) * duration) / (8 * 1024 * 1024):.1f} MB"
        )

    if codec.lower() in ("av1", "svtav1"):
        preset = "8"
    elif codec.lower() in ("h265", "hevc"):
        preset = "fast"
    else:
        preset = "ultrafast"

    num_cores = min(os.cpu_count() or 2, MAX_FFMPEG_THREADS)
    num_segments = 1

    tmpdir = tempfile.mkdtemp(prefix="turbo_compress_")

    try:
        attempt = 0
        max_attempts = 3

        while attempt < max_attempts:
            attempt += 1

            if progress_callback:
                await progress_callback(
                    f"Сжатие (попытка {attempt}/{max_attempts}): подготовка..."
                )

            if num_segments > 1:
                seg_dir = os.path.join(tmpdir, f"segments_{attempt}")
                os.makedirs(seg_dir, exist_ok=True)

                segments = await _split_video_segments(
                    input_path, seg_dir, num_segments, duration
                )

                if len(segments) <= 1:
                    if progress_callback:
                        await progress_callback(
                            f"Сжатие (попытка {attempt}): 0%"
                        )
                    rc = await _encode_segment(
                        input_path, output_path, video_bitrate, audio_bitrate,
                        codec, preset, num_cores, duration, progress_callback,
                        label=f"Сжатие (попытка {attempt})",
                    )
                    if rc != 0:
                        raise RuntimeError(f"FFmpeg encode failed (attempt {attempt})")
                else:
                    threads_per = max(1, num_cores // len(segments))
                    encoded_paths = []

                    sem = asyncio.Semaphore(MAX_PARALLEL_SEGMENTS)

                    async def _encode_with_limit(seg, enc_path):
                        async with sem:
                            seg_dur = seg["end"] - seg["start"]
                            return await _encode_segment(
                                seg["path"], enc_path,
                                video_bitrate, audio_bitrate,
                                codec, preset, threads_per, seg_dur,
                                progress_callback if seg["index"] == 0 else None,
                                label=f"Сег. {seg['index']+1}/{len(segments)} (поп. {attempt})",
                            )

                    enc_tasks = []
                    for seg in segments:
                        enc_path = seg["path"].replace(".mkv", "_enc.mp4")
                        encoded_paths.append(enc_path)
                        enc_tasks.append(_encode_with_limit(seg, enc_path))

                    if progress_callback:
                        await progress_callback(
                            f"Сжатие {len(segments)} сегментов (попытка {attempt})..."
                        )

                    results = await asyncio.gather(*enc_tasks)

                    for i, rc in enumerate(results):
                        if rc != 0:
                            raise RuntimeError(f"Segment {i} encode failed (attempt {attempt})")

                    if progress_callback:
                        await progress_callback("Склеиваю сегменты...")

                    valid_paths = [p for p in encoded_paths if os.path.exists(p) and os.path.getsize(p) > 0]
                    if not valid_paths:
                        raise RuntimeError("All segments failed")

                    for seg in segments:
                        try:
                            if os.path.exists(seg["path"]):
                                os.remove(seg["path"])
                        except OSError:
                            pass

                    rc = await _concat_segments(valid_paths, output_path)
                    if rc != 0:
                        raise RuntimeError(f"Concatenation failed (attempt {attempt})")

                    for p in valid_paths:
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except OSError:
                            pass
            else:
                if progress_callback:
                    await progress_callback(
                        f"Сжатие (попытка {attempt}): 0%"
                    )
                rc = await _encode_segment(
                    input_path, output_path, video_bitrate, audio_bitrate,
                    codec, preset, num_cores, duration, progress_callback,
                    label=f"Сжатие (попытка {attempt})",
                )
                if rc != 0:
                    raise RuntimeError(f"FFmpeg encode failed (attempt {attempt})")

            output_size = os.path.getsize(output_path)
            logger.info(
                f"Attempt {attempt}: target={target_bytes}, result={output_size}, "
                f"vbitrate={video_bitrate}, segments={num_segments}"
            )

            if output_size <= target_bytes:
                return {
                    "success": True,
                    "input_size": input_size,
                    "output_size": output_size,
                    "target_size": target_bytes,
                    "duration": duration,
                    "codec": codec,
                    "attempts": attempt,
                    "segments_used": num_segments if num_segments > 1 else 1,
                }

            ratio = target_bytes / output_size
            video_bitrate = int(video_bitrate * ratio * 0.85)
            if video_bitrate < 50_000:
                video_bitrate = 50_000
            num_segments = 1
            logger.warning(
                f"Output {output_size} > target {target_bytes}, "
                f"reducing bitrate to {video_bitrate} for retry"
            )

        output_size = os.path.getsize(output_path)
        if output_size > target_bytes:
            raise RuntimeError(
                f"Could not compress to {target_mb} MB in {max_attempts} attempts. "
                f"Final size: {output_size / (1024*1024):.2f} MB"
            )

        return {
            "success": True,
            "input_size": input_size,
            "output_size": output_size,
            "target_size": target_bytes,
            "duration": duration,
            "codec": codec,
            "attempts": attempt,
            "segments_used": 1,
        }

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
