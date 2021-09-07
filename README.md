# AudioSet_downloader
A repository to download AudioSet database

<br>

**Sep 07, 2021**
* First release of the project.

<br>

## Dependency installation

The code was successfully built and run with these versions:

```
soundfile 0.10.3
tqdm	4.61.2
moviepy	1.0.3
joblib	1.0.1

```
Note: You can also create the environment I've tested with by importing _environment.yml_ to conda.


## Downloading

You can download the database with running main.py . 


```
usage: main.py

optional arguments:
  -h, --help                Show this help message and exit
  --mode                    Select the type of the data you are interested in.
  -d, --destination_dir     directory path to put downloaded files into
  --label_file              Path to CSV file containing AudioSet labels for each class
  --csv_dataset             Path to CSV file containing AudioSet in YouTube-id/timestamp form
```

<br>



<br><br><br>

