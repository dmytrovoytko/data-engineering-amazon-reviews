# Data Engineering project Amazon Reviews

Data Engineering project for ZoomCamp`24: JSONL -> PostgreSQL + Metabase + Mage.AI

ETL for `Amazon Reviews'23` dataset. Source: https://amazon-reviews-2023.github.io/ 
Cloud Environment: GitHub CodeSpaces, free and enough to test.

## 🎯 Goals

![Reviews by Main category](/screenshots/pie-chart-reviews-by-main-category.png)


## 🚀 Instructions to deploy and test

💡 Oh, so many steps?! Please don't panic, they are all simple enough, that's why so many. One step at a time. I did it, you can do it too! Let's go!

### 🛠️🧰 Setup environment

1. Fork this repo in GitHub.
2. Create GitHub CodeSpace from the repo.
3. Start CodeSpace, wait for `requirements.txt` installation finish (starts automatically, just watch & wait).
4. Copy `dev.env` to `.env` - run `cp dev.env .env` in your CodeSpace terminal. You don't need to change anything to test the process. It contains **all key settings** (PostgreSQL etc).
```bash
cp dev.env .env
```

### ⬇️🗂 Download dataset and start PostgreSQL server

5. Run `bash download.sh` to download 1st dataset (smallest) - from `dataset_urls0.csv`. As a result you will have 2 files in your `data` directory: `meta_Digital_Music.jsonl.gz` and `meta_Digital_Music.jsonl.gz`. Cool!✅
```bash
bash download.sh
```
6. Start PostgreSQL docker: `bash start_postgres.sh`. It will automatically download postgres container and start it.
```bash
bash start_postgres.sh
```
7. Wait until you see the message `database system is ready to accept connections` - PostgreSQL is ready to serve you. Let it run in this terminal so you can see logs and easily stop it later.

8. Start 2nd terminal (click `+` button on the panel in the right corner above terminal) and switch to it. 
9. You can check database connection by running `pgcli -h 172.17.0.2 -p 5432 -u postg -d amzn_reviews`, password `postg`. All dockerized apps of this setup are suppose to run in default `bridge` network, `172.17.0.x`, with `172.17.0.2` for PostgreSQL. If this step fails you're in trouble 😅 But, when you run it in a CodeSpace and follow the instruction it should work fine, I tested multiple times. Type `quit` to return to terminal CLI. Ok, PostgreSQL is running.✅
```bash
pgcli -h 172.17.0.2 -p 5432 -u postg -d amzn_reviews
```

### ➡️📦️ Ingest data using CLI (bash & python)

10. Run `bash process.sh` to ingest dataset files into PostgreSQL database. 
```bash
bash process.sh
```
It executes python script `jsonl_postgres.py` with default parameters from your `.env` settings. By default (without parameter) it processes 1st dataset files. As the result of successful loading you will see some progress messages and finally `Finished ingesting data/... into the PostgreSQL database! Total time ... second(s) +++ PROCESSING finished: OK!`. Congratulations, the first approach to load data accomplished and you have records in 2 tables of your database: `meta` with products, `reviews` with ratings.✅ 

11. If you don't want (or have no time) to play with Mage.AI orchestration, you can ingest few more datasets with the same approach: 
- download 2nd dataset: run `bash download.sh dataset_urls1.csv` (Health_and_Personal_Care.jsonl files)
```bash
bash download.sh dataset_urls1.csv
```
- process 2nd dataset: run `bash process.sh dataset_urls1.csv` 
```bash
bash process.sh dataset_urls1.csv
```
- repeat these steps for larger datasetsL `dataset_urls2.csv` for All_Beauty, `dataset_urls3.csv` for Software.

### 📊 Visualize data

Finally, probably the most interesting part, let's see our data using Metabase - open sourse and free self-hosted Business Intelligence, Dashboards and Data Visualization tool. 

![Reviews by Verified purchase](/screenshots/reviews-by-verified-purchase-monthly.png)

12. Start it in Docker - run 
```bash
bash start_metabase.sh
```
It will automatically download Metabase container and start it.

13. CodeSpace will pop-up the notification that `Your application running on port 3000 is available.` - click `Open in Browser`. New page would probably open white. Please wait a couple of seconds to let it start, then refresh the page. Now you will see login screen. Just login with `john@mailinator.com`, pass: `Jj123456` (no worries, it's self-hosted, all safe). Your visualizations are already there for you!✅

14. Explore the dashboard on the main screen. There are 2 tabs: `Products` and `Reviews`.
15. **Products**: you can see 2 reports:
- pie chart with number of Products by Main category
- Products number distribution by Average rating 
16. **Reviews**: you can see 4 reports:
- pie chart with Reviews number by Verified/not purchase
- Reviews rating number distribution by Verified/not purchase over time (by months)  
- pie chart with Reviews number by Main category
- Reviews rating number distribution by Main category over time (by months)  

17. The more datasets you load, the more categories you can see. So I offer you to download and process at least 2 datasets. You can do it by following steps in [Ingest data using CLI (bash & python)](#ingest-data-using-cli-bash--python) or go ahead and discover [Mage AI](#data-ingestion-orchestration-mage-ai).

### 🧙‍♀️ Data ingestion orchestration (Mage AI)

I wouldn't say it was so simple and easy as Matt showed us in videos, but with some time and effort I managed to find the way to convert the logic of `jsonl_postgres.py` script into Mage pipelines and bricks. Why it worth time? Because larger datasets (like Kindle_Store) will probably demand a serious cloud storage and database than free CodeSpace playground. And I (you?) need to learn how to deal with them with a more scalable system, providing long job execution, partitioning, monitoring and logs. So let's see 2 pipelines I managed to setup with Mage. 

18. 


