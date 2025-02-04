
## Running apache airflow 2.0 in docker with local executor.
Here are the steps to take to get airflow 2.0 running with docker on your machine. 
1. Clone this repo
1. Create dags, logs and plugins folder inside the project directory
```bash
mkdir ./dags ./logs ./plugins
```
1. Set user permissions for Airflow to your current user
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
1. Install docker desktop application if you don't have docker running on your machine
- [Download Docker Desktop Application for Mac OS](https://hub.docker.com/editions/community/docker-ce-desktop-mac)
- [Download Docker Desktop Application for Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows)
1. Launch airflow by docker-compose
```bash
docker-compose up -d
```
1. Check the running containers
```bash
docker ps
```
1. Open browser and type http://0.0.0.0:8080 to launch the airflow webserver


## About Airflow worker parallelism testing

1. 
    when set up the parallelism as below on a laptop with 16 GB RAM, Ultra 7 165H CPU.
    ```docker-compose
    ...
    environment:
        AIRFLOW__CORE__PARALLELISM: 16
        AIRFLOW__CORE__DAG_CONCURRENCY: 16
        AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
    ...
    ```
    The [DAG](dags/hawkeye.py) tested against 123 local json file with dynamic task mapping would take 38 secs to complete.

    when set up the parallelism as this:
    ```docker-compose
        AIRFLOW__CORE__PARALLELISM: 50
        AIRFLOW__CORE__DAG_CONCURRENCY: 50
    ```
    The same DAG tested against 123 local json file with dynamic task mapping taks the same 38 secs to complete.


2. 
    Then we increased the json file to 1150. The total time comes to 3 mins 18 secs, with around 5 G RAM usage.
    For parallelism 50, the total time comes to 1 mins 41 secs, with around 10 G RAM usage. If we set the batch number from 500 to 100, then the time can further reduce to 1 mins 14 secs.

3. 
    With total file of 72531, the initial test took 3:06:42. After adjusting batch number and parallelism to make the batch number can be divided by parallelism evenly, and use opt-lvl 3 for the rust app building, the total time comes to 1:13:57.