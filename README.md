# PySpark Data Piepeline - Aviation data

Json data API : https://aviationstack.com/documentation

- Create an account and get your API key 

### What it does for the moment ?

- Get daily data

- Cleaning data (handle nulls, bad schema, naming etc.)

- Save it to parquet for further analysis

Try it :

- Create your venv : 
```bash 
   python3 -m venv venv
```
- Get the requirements in utils folder :
```bash 
   pipreqs . > requirements.txt
```
- Activate your venv :
```bash 
   venv venv/bin/activate
```
- Add your API key to your .env 
- Finally run the main.py 

### TODO :

- Airflow scheduler : schedule daily ETL
- Database : use bronze, silver and gold database
- Processing : Aggregate data to create usefull datamarts in gold table 
- Enhance logging and code modularity