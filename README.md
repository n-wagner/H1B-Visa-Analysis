# H1B-Visa-Analysis
Google Cloud DataProc analysis of H1B visa data for ECE 795 Advanced Big Data Analytics.

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

1. SSH into cluster (in `Command Prompt`)
    1. `set PROJECT=<PROJECT_ID> && set HOSTNAME=<MASTER_CLUSTER> && set ZONE=<CLUSTER_ZONE> && set PORT=<PORT_VALUE>`
    2. `gcloud compute ssh %HOSTNAME% --project=%PROJECT% --zone=%ZONE%  -- -D %PORT%`
2. Create local file
    1. `nano project.py`
    2. Paste a copy of `main.py` by right clicking
    3. `ctrl+x` to save
3. Consider flags `python project.py --help`
4. Run default or with flags (e.g. `python project.py`, `python project.py --force -no-basic`, etc.)
