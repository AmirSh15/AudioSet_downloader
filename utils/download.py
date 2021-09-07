import time

import pafy, ffmpy, os, ffmpeg, shutil
import numpy as np
from tqdm import tqdm
import soundfile as sf
from moviepy.video.io.ffmpeg_tools import ffmpeg_extract_subclip
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from moviepy.video.io.VideoFileClip import VideoFileClip
from joblib import delayed, Parallel # install psutil library to manage memory leak


def download_a_video_audio(faulty_files, data_dir, audio_id, url, labels, start_time=None, end_time=None,
                           mode='video', timeout=1):

    download_status = False
    # directory for saving temporary files
    tmp_dir = './tmp'

    if(end_time-start_time!=10):
        faulty_files.append(f'{audio_id+4} {start_time} {end_time} {labels} {url}')
        return False
    try:
        video = pafy.new(url)
        tile = video.title
        bestvideo = video.getbestvideo(preftype='mp4')
        bestaudio = video.getbestaudio()
        if start_time!= None:
            audio_path = tmp_dir + f'/audio_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.{bestaudio.extension}'
            video_path = tmp_dir + f'/video_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.{bestvideo.extension}'
        else:
            audio_path = tmp_dir + f'/audio_{audio_id}.{bestaudio.extension}'
            video_path = tmp_dir + f'/video_{audio_id}.{bestvideo.extension}'
        print(f"start to download {url}")
        if mode == 'video':
            bestaudio.download(filepath=str(audio_path))
            bestvideo.download(filepath=str(video_path))
        elif mode == 'audio':
            bestvideo.download(filepath=str(video_path))
        print(f"end to download {url}")

        # change audio format
        a_extension = bestaudio.extension
        xindex = audio_path.find(a_extension)
        audioname = audio_path[0:xindex]

        if a_extension not in ['wav']:
            conv2wav = ffmpy.FFmpeg(
                inputs={audioname + a_extension: None},
                outputs={audioname + 'wav': None},
                global_options={'-loglevel quiet'}
            )
            conv2wav.run()
            while os.path.isfile(audioname + a_extension):
                os.remove(audioname + a_extension)

        if (start_time!=None):
            file = audioname + 'wav'
            aud_data, sample_rate = sf.read(file)

            total_time = len(aud_data) / sample_rate
            if end_time>total_time:
                start_point = np.max([int(sample_rate * (start_time - 1)), 0])
                end_point = int(sample_rate * (end_time - 1))
            else:
                start_point = int(sample_rate * start_time)
                end_point = int(sample_rate * end_time)


            if mode == 'video':
                # change video format
                v_extension = bestvideo.extension
                xindex = video_path.find(v_extension)
                videoname = video_path[0:xindex]

                if (start_time != None):
                    video_path_tmp = videoname + '_.mp4'
                    with VideoFileClip(videoname+v_extension) as video:
                        new = video.subclip(start_time, end_time)
                        new.write_videofile(video_path_tmp, audio_codec='aac')
                    while os.path.isfile(videoname+v_extension):
                        os.remove(videoname+v_extension)
        # save files into their directories
        for label in labels:
            audio_path = data_dir + f'/{label}/audio_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.wav'
            video_path = data_dir + f'/{label}/video_{audio_id}_start_{int(start_time)}_end_{int(end_time)}.mp4'
            sf.write(audio_path, aud_data[start_point:end_point], sample_rate)
            shutil.copy(video_path_tmp, video_path)

        # make sure that the temp files are removed
        while os.path.isfile(audioname + 'wav') and os.path.isfile(videoname + '_.mp4'):
            os.remove(audioname + 'wav')
            os.remove(videoname + '_.mp4')

        download_status = True

    except Exception as e:
        print(e)
        faulty_files.append(f'{audio_id + 4} {start_time} {end_time} {labels} {url}')
        download_status = False

    return download_status

def parallel_download(data, args):

    steps = 10
    # directory for saving temporary files
    tmp_dir = './tmp'

    # text file for saving failure cases
    faulty_files = []

    # id = 1
    # download_a_video_audio(faulty_files, args.destination_dir, data.index[id], data.url[id], data.lables[id],
    #                        data.start[id], data.end[id], data.download_status[id])

    if os.path.isdir(tmp_dir) == False:
        os.mkdir(tmp_dir)
    data.download_status = Parallel(n_jobs=steps)(delayed(download_a_video_audio)(faulty_files,
        args.destination_dir, data.index[id], data.url[id], data.lables[id],
        data.start[id], data.end[id], args.mode) for id in tqdm(range(len(data.index[:20])), total=len(data.index[:20])))

    faulty_files = [f'{data.index[id] + 4} {data.start[id]} {data.end[id]} {data.lables[id]} {data.url[id]}' for id, status in enumerate(data.download_status) if status == False]

    np.savetxt('faulty_files.txt', faulty_files, fmt='%s')


