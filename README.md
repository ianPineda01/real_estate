# real_estate
Python project that takes the url from an [Inmuebles 24](https://www.inmuebles24.com/)
query and returns a spark dataframe with useful information about the units in said
query

## Set up virtual environment and install dependencies
```
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Run the program
```
python main.py [url] [number of pages to scrap for info (optional, default = 1)]
```

Note: for every aditional page, we wait 500 miliseconds to not overwhelm the website
with our requests.

## Exit virtual environment
```
deactivate
```