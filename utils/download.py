import time

import pafy, ffmpy, os, ffmpeg, shutil
from pytube import YouTube
import numpy as np
from tqdm import tqdm
import soundfile as sf
from moviepy.video.io.VideoFileClip import VideoFileClip
from joblib import delayed, Parallel  # install psutil library to manage memory leak


def download_a_video_audio(
    faulty_files,
    data_dir,
    audio_id,
    url,
    labels,
    start_time=None,
    end_time=None,
    mode="video",
    timeout=1,
    verbose=False,
):

    download_status = False
    # directory for saving temporary files
    tmp_dir = "./tmp"

    if end_time - start_time != 10:
        faulty_files.append(f"{audio_id+4} {start_time} {end_time} {labels} {url}")
        return False
    try:
        # video = pafy.new(url)
        # tile = video.title
        # bestvideo = video.getbestvideo(preftype="mp4")
        # bestaudio = video.getbestaudio()

        youtube = YouTube(url)
        audio = youtube.streams.filter(only_audio=True).asc().first()
        audio_ext = audio.mime_type.split("/")[-1]
        if mode == "video":
            video = youtube.streams.filter(progressive=True, file_extension="mp4").asc().first()
        elif mode == "only_video":
            video = youtube.streams.filter(only_video=True).asc().first()
        video_ext = video.mime_type.split("/")[-1]

        if start_time != None:
            audio_path = (
                tmp_dir
                + f"/audio_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.{audio_ext}"
            )
            video_path = (
                tmp_dir
                + f"/video_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.{video_ext}"
            )
        else:
            audio_path = tmp_dir + f"/audio_{audio_id}.{audio_ext}"
            video_path = tmp_dir + f"/video_{audio_id}.{video_ext}"

        if verbose:
            print(f"start to download {url}")

        if mode == "both_separate":
            # bestaudio.download(filepath=str(audio_path))
            # bestvideo.download(filepath=str(video_path))
            video = video.download(tmp_dir)
            os.rename(video, video_path)
            audio = audio.download(tmp_dir)
            os.rename(audio, audio_path)
        elif mode == "only_audio":
            audio = audio.download(tmp_dir)
            os.rename(audio, audio_path)
        elif mode == "video" or mode == "only_video":
            video = video.download(tmp_dir)
            os.rename(video, video_path)

        if verbose:
            print(f"end to download {url}")

        # change audio format
        a_extension = audio_ext
        xindex = audio_path.find(a_extension)
        audioname = audio_path[0:xindex]

        if a_extension not in ["wav"] and (mode != "video" and mode != "only_video"):
            conv2wav = ffmpy.FFmpeg(
                inputs={audioname + a_extension: None},
                outputs={audioname + "wav": None},
                global_options={"-loglevel quiet"},
            )
            conv2wav.run()
            while os.path.isfile(audioname + a_extension):
                os.remove(audioname + a_extension)

        if start_time != None:
            if mode == "only_audio" or mode == "both_separate":
                file = audioname + "wav"
                aud_data, sample_rate = sf.read(file)

                total_time = len(aud_data) / sample_rate
                if end_time > total_time:
                    start_point = np.max([int(sample_rate * (start_time - 1)), 0])
                    end_point = int(sample_rate * (end_time - 1))
                else:
                    start_point = int(sample_rate * start_time)
                    end_point = int(sample_rate * end_time)

            if mode == "both_separate" or mode == "video" or mode == "only_video":
                # change video format
                v_extension = video_ext
                xindex = video_path.find(v_extension)
                videoname = video_path[0:xindex]

                if start_time != None:
                    video_path_tmp = videoname + "_.mp4"
                    with VideoFileClip(videoname + v_extension) as video:
                        new = video.subclip(start_time, end_time)
                        new.write_videofile(video_path_tmp, audio_codec="aac", logger=None)
                    while os.path.isfile(videoname + v_extension):
                        os.remove(videoname + v_extension)

        # save files into their directories
        for label in labels:
            audio_path = (
                data_dir
                + f"/{label}/audio_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.wav"
            )
            video_path = (
                data_dir
                + f"/{label}/video_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.mp4"
            )

            if mode == "only_audio" or mode == "both_separate":
                sf.write(audio_path, aud_data[start_point:end_point], sample_rate)
            if mode == "both_separate" or mode == "video" or mode == "only_video":
                shutil.copy(video_path_tmp, video_path)

        # make sure that the temp files are removed
        while os.path.isfile(audioname + "wav"):
            os.remove(audioname + "wav")
        while os.path.isfile(videoname + "_.mp4"):
            os.remove(videoname + "_.mp4")

        download_status = True

    except Exception as e:
        if verbose:
            print(f"Error: {e}")
        faulty_files.append(f"{audio_id + 4} {start_time} {end_time} {labels} {url}")
        faulty_files.append(e)
        download_status = False

    return download_status


def parallel_download(data, args):

    steps = 10
    # directory for saving temporary files
    tmp_dir = "./tmp"

    # text file for saving failure cases
    faulty_files = []

    # id = 1
    # download_a_video_audio(
    #     faulty_files=faulty_files,
    #     data_dir=args.destination_dir,
    #     audio_id=data.index[id],
    #     url=data.url[id],
    #     labels=data.lables[id],
    #     start_time=data.start[id],
    #     end_time=data.end[id],
    #     mode=args.mode,
    # )

    if os.path.isdir(tmp_dir) == False:
        os.mkdir(tmp_dir)
    data.download_status = Parallel(n_jobs=steps)(
        delayed(download_a_video_audio)(
            faulty_files,
            args.destination_dir,
            data.index[id],
            data.url[id],
            data.lables[id],
            data.start[id],
            data.end[id],
            args.mode,
            args.verbose,
        )
        for id in tqdm(range(len(data.index)), total=len(data.index), desc="Downloading")
    )

    faulty_files = [
        f"{data.index[id] + 4} {data.start[id]} {data.end[id]} {data.lables[id]} {data.url[id]}"
        for id, status in enumerate(data.download_status)
        if status == False
    ]

    np.savetxt("faulty_files.txt", faulty_files, fmt="%s")
