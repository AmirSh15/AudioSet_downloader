import os, shutil
import argparse
import time

from utils.utils import read_csv, Data
from utils.download import parallel_download


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        type=str,
        choices=["audio", "video"],
        help="select the type of the data you are interested in.",
    )
    parser.add_argument(
        "-c",
        "--classes",
        nargs="+",
        type=str,
        help="list of classes to find in a given directory of audioset files",
    )
    parser.add_argument(
        "-b",
        "--blacklist",
        nargs="+",
        type=str,
        help="list of classes which will exclude a clip from being downloaded",
    )
    parser.add_argument(
        "-d",
        "--destination_dir",
        type=str,
        help="directory path to put downloaded files into",
    )
    parser.add_argument(
        "-fs",
        "--sample_rate",
        type=int,
        help="Sample rate of audio to download. Default 16kHz (only applicable in audio mode)",
    )
    parser.add_argument(
        "--label_file",
        type=str,
        help="Path to CSV file containing AudioSet labels for each class",
    )
    parser.add_argument(
        "--csv_dataset",
        type=str,
        help="Path to CSV file containing AudioSet in YouTube-id/timestamp form",
    )

    parser.set_defaults(
        mode="video",
        classes=None,
        blacklist=None,
        destination_dir="./AudioSet",
        fs=16000,
        label_file="./Data_list/labels.csv",
        csv_dataset="./Data_list/eval_segments.csv",
    )

    args = parser.parse_args()

    # Load data
    AudioSet = Data(args.csv_dataset, args.label_file, args.classes, args.blacklist)

    # Creat destination folders
    if os.path.isdir(args.destination_dir) == False:
        os.mkdir(args.destination_dir)
        for folder in AudioSet.classes_name:
            os.mkdir(os.path.join(args.destination_dir, folder))

    parallel_download(AudioSet, args)
