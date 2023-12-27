## How to use
(Tested using python 3.10.12)

In a terminal run the following commands
``` bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Create database
Download the .csv file from [https://www.kaggle.com/datasets/sahityasetu/crime-data-in-los-angeles-2020-to-present/data]
``` bash
python3 write_to_db.py
```

## Run server
``` bash
python3 server.py
```

