# H1B-Visa-Analysis
Google Cloud DataProc analysis of H1B visa data for ECE 795 Advanced Big Data Analytics.

## Pre-Installation

1. Make sure you have a Google Cloud account with billing enabled
2. Create a project
3. Create a DataProc cluster
4. Upload the CSV from [here](https://www.kaggle.com/nsharan/h-1b-visa) to Google Cloud Storage
5. Download Google Cloud SDK (version `336.0.0`)

## Installation

**Locally:**
```sh
> py -3.8 --version
Python 3.8.5
```
**On DataProc:**
```sh
$ python --version
Python 3.8.8
```
### For Local Development (Using VSCode)

1. **Optional** - Create python virtual environment
    1. `py -3.8 -m venv venv`<br>
        OR (make sure the below is the right version)<br>
        `python3 -m venv venv`
    2. **Win:** `./venv/Scripts/activate`<br>
       **Linux:** `source ./venv/bin/activate`
    3. To leave (when done running code): `deactivate`
2. Install dependencies
    1. `pip install -r requirements.txt`

## Running

1. SSH into cluster (in `Command Prompt`, [reference](https://cloud.google.com/dataproc/docs/concepts/accessing/cluster-web-interfaces#set_commonly_used_command_variables))
    1. `set PROJECT=<PROJECT_ID> && set HOSTNAME=<MASTER_CLUSTER_NAME> && set ZONE=<CLUSTER_ZONE> && set PORT=<PORT_VALUE>`<br>
        The values between `< >` should be replaced with their respective values - see the reference if there is confusion.
    2. `gcloud compute ssh %HOSTNAME% --project=%PROJECT% --zone=%ZONE%  -- -D %PORT%`
        ```
        > gcloud --version
        Google Cloud SDK 336.0.0
        bq 2.0.66
        core 2021.04.09
        gsutil 4.61
        ```
2. Create local file
    1. `nano project.py`
    2. Paste a copy of `main.py` by right clicking
    3. `ctrl+x` to save
3. Consider flags `python project.py --help`
    ```console
    $ python project.py --help
    usage: project.py [-h] [-f] [-q] [--hdfs HDFS] [--dataset DATASET] [-s SOURCE] [--table TABLE] [--no-basic] [--no-additional] [--no-task]

    H1B Visa Petition Analysis

    optional arguments:
    -h, --help            show this help message and exit
    -f, --force           always perform data transfers
    -q, --quiet           do not print notifications
    --hdfs HDFS           specify a HDFS directory to store data
    --dataset DATASET     specify a Google Cloud dataset name
    -s SOURCE, --source SOURCE
                            specify the path to a source data CSV file in Google Cloud Storage
    --table TABLE         specify a Google Cloud dataset table name
    --no-basic            do not execute basic queries
    --no-additional       do not execute additional queries
    --no-task             do not execute task queries
    --no-timing           do not execute timing queries
    ```
4. Run default or with flags (e.g. `python project.py`,<br>
   `python project.py --force --no-basic`, etc.)
