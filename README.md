# Data Engineering project Amazon Reviews

Data Engineering project for ZoomCamp`24: JSONL -> PostgreSQL + Metabase + Mage.AI

ETL for [Amazon Reviews'23` dataset](https://amazon-reviews-2023.github.io/).

![Data Engineering project Amazon Reviews](/screenshots/amazon-reviews-data-engineering.png)

Cloud environment: **GitHub CodeSpaces**, free and enough to test (120 core-hours of Codespaces compute for free monthly, 15GB of Codespaces storage for free monthly). 

To reproduce and review this project it would be enough (hopefully) less than hour, and ~2GB total for described datasets. You don't need to use anything extra, like VS Code or trial accounts - just webrowser + GitHub account is totally enough.

This level of cloud resources allowed me to process and analyze-visualize datasets with at least 6 Mln reviews without issues.

## 🎯 Goals

This is my Data Engineering project started during [DE ZoomCamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)'24.
And **the main goal** is straight-forward: **Extract - Transform - Load** data, then **visualize** some insights.  

I chose to analyze [Amazon Reviews](https://amazon-reviews-2023.github.io/) dataset. Full dataset is huge, and it is available to download in parts - by product categories. There are some smaller and larger sub-datasets, some of them we would explore here (Digital_Music, Health_and_Personal_Care, All_Beauty, Software). Each subset includes 2 files: products (**meta**), ratings and user comments (**reviews**).

Some time ago I became very interested in how much we can trust all those ratings on Amazon, including 'bestseller' statuses. But without proper data it was hard to investigate. Now, years later, I have more skills in data analytics and this huge dataset, thanks to [McAuley Lab](https://cseweb.ucsd.edu/~jmcauley/). I chose to use only part of the whole information: 
- **Products**: categories and average ratings 
- **Reviews**: user's ratings, review dates, was that verified purchase or not 
  (will analyze reviews texts later, awesome source for many insights I think)

Thanks to ZoomCamp for the reason to learn many new tools and get back to my 'reviews & trust' questions!  

### 🕵️ Questions that I chose to investigate during this project:

- What are the trends in reviews ratings for verified/not purchases? Are they more negative or more positive than average?
- As not verified purchase reviews can be manipulative, what is their ratio in total number?
- As automated review submission is technically possible with more advanced tech last years, what are the not verified purchase rating trends over time?
- Are there any significant differences in trends for product categories?

Let's explore together! These categories (datasets) I played with so far.

![Reviews by Main category](/screenshots/pie-chart-reviews-by-main-category.png)

## ℹ️ Assets

- dataset urls are defined in .csv files stored in `/data` directory - `dataset_urls0.csv`, `dataset_urls1.csv`, etc
- my scripts (bash, python, sql)
- pre-configured Metabase dashboard
- pre-configured Mage AI pipelines 

## :toolbox: Tech stack

- PostgreSQL as a data base/warehouse - open source and free
- [Metabase](https://www.metabase.com/) as an analitics platform - open source and free self-hosted
- [Mage AI](https://www.mage.ai/) as an orchestration tool - open source and free self-hosted

💡 Combination of CodeSpaces + PostgreSQL + Metabase + Mage AI would be a good choice for those who prefer/experiment with a simple and open source approach, or those who hesitate to deal with The BIG 3 cloud providers (AWS, Azure, GCP) with their serious payments. Looks like a good fit for early stage startups, developers, and those who learn Data Engineering as me (you?).  

## 🚀 Instructions to deploy

🙈 Oh, so many steps?! Please don't panic, they are all simple enough, that's why so many. One step at a time. I did it, you can do it too! Let's go!

- [Setup environment](#hammer_and_wrench-setup-environment)
- [Download dataset and start PostgreSQL server](#arrow_heading_down-download-dataset-and-start-postgresql-server)
- [Ingest data using CLI (bash & python)](#keyboard-ingest-data-using-cli-bash--python)
- [Visualize data](#-visualize-data)
- [Orchestration data ingestion (Mage AI)](#mage-orchestrate-data-ingestion-mage-ai)
- [Instructions to stop the apps](#stop_button-instructions-to-stop-the-apps)

### :hammer_and_wrench: Setup environment

1. Fork this repo on GitHub.
2. Create GitHub CodeSpace from the repo.
3. **Start CodeSpace**, wait for `requirements.txt` installation to finish (it starts automatically, just watch & wait).
4. Copy `dev.env` to `.env` - run `cp dev.env .env` in your CodeSpace terminal. You don't need to change anything to test the process. It contains **all key settings** (PostgreSQL etc).
```bash
cp dev.env .env
```

### :arrow_heading_down: Download dataset and start PostgreSQL server

5. Run `bash download.sh` to download 1st dataset (smallest) - from `dataset_urls0.csv`. As a result you will have 2 files in your `data` directory: `meta_Digital_Music.jsonl.gz` and `meta_Digital_Music.jsonl.gz`. Cool! ✅
```bash
bash download.sh
```
6. Start PostgreSQL docker: `bash start_postgres.sh`. It will automatically download postgres container and start it.
```bash
bash start_postgres.sh
```
7. Wait until you see the message `database system is ready to accept connections` - PostgreSQL is ready to serve you. 

![PostgreSQL is ready](/screenshots/postgresql-is-ready.png)

Let it run in this terminal so you can see logs and easily stop it later (with just Ctrl-C). CodeSpace will pop-up the notification that `Your application running on port 5432 is available.` - just ignore it (close that pop-up).

8. Start 2nd terminal (click `+` button on the panel in the right corner above terminal) and switch to it. 
9. You can check database connection by running `pgcli -h 172.17.0.2 -p 5432 -u postg -d amzn_reviews`, password `postg`. All dockerized apps of this setup are suppose to run in default `bridge` network, `172.17.0.x`, with `172.17.0.2` for PostgreSQL. If this step fails you're in trouble 😅 But, when you run it in a CodeSpace and follow the instruction it should work fine, I tested multiple times. Type `quit` to return to terminal CLI. Ok, PostgreSQL is running. ✅
```bash
pgcli -h 172.17.0.2 -p 5432 -u postg -d amzn_reviews
```

### :keyboard: Ingest data using CLI (bash & python)

Dataset files have been downloaded, PostgreSQL is running - time to ingest your data!

10. Run `bash process.sh` to ingest dataset files into PostgreSQL database. 
```bash
bash process.sh
```
It executes python script `jsonl_postgres.py` with default parameters from your `.env` settings. By default (without parameter) it processes 1st dataset files. As the result of successful loading you will see some progress messages and finally `Finished ingesting data/... into the PostgreSQL database! Total time ... second(s) +++ PROCESSING finished: OK!`. 

![Processing successful](/screenshots/processing-successful.png)

Congratulations, the first approach to load data accomplished and you have records in 2 tables of your database: `meta` with products, `reviews` with ratings. ✅ 

11. Now you can ingest the next dataset with the same approach: 
- download 2nd dataset: run `bash download.sh dataset_urls1.csv` (Health_and_Personal_Care files)
```bash
bash download.sh dataset_urls1.csv
```
- process 2nd dataset: run `bash process.sh dataset_urls1.csv` 
```bash
bash process.sh dataset_urls1.csv
```
- download 3rd dataset: run `bash download.sh dataset_urls2.csv` (All_Beauty files)
```bash
bash download.sh dataset_urls2.csv
```

If you want to try data workflow orchestration have some patience and follow to [Visualize data](#-visualize-data) step. If you don't want (or have no time) to play with Mage.AI, just process similarly `dataset_urls2.csv` for All_Beauty, and download then process `dataset_urls3.csv` for Software.

### 📊 Visualize data

Finally, probably the most interesting part, let's see our data using Metabase - open sourse and free self-hosted Business Intelligence, Dashboards and Data Visualization tool. 

![Reviews by Verified purchase](/screenshots/reviews-by-verified-purchase-monthly.png)

12. Start it in Docker - run 
```bash
bash start_metabase.sh
```
It will automatically download Metabase container and start it.

13. CodeSpace will pop-up the notification that `Your application running on port 3000 is available.` - click `Open in Browser`. 

![Metabase app pop-up](/screenshots/metabase-app-pop-up.png)

New page would probably open white. Please wait a couple of seconds to let it start, then refresh the page. Now you will see login screen. Just login with `john@mailinator.com`, pass: `Jj123456` (no worries, it's self-hosted, all safe). Your visualizations are already there for you! ✅🎉

💡 In case you accidentally close that pop-up or Metabase page and you need it later (after ingesting new datasets), you can always open that page from `Ports` tab:

![Metabase app Ports](/screenshots/metabase-app-ports.png)

14. Explore the dashboard on the main screen. There are 2 tabs: `Products` and `Reviews`.
15. **Products**: you can see 2 reports:
- pie chart with number of Products by Main category
- Products number distribution by Average rating 
16. **Reviews**: you can see 4 reports:
- pie chart with Reviews number by Verified/not purchase
- Reviews rating number distribution by Verified/not purchase over time (by months)  
- pie chart with Reviews number by Main category
- Reviews rating number distribution by Main category over time (by months) 

You can see some [screenshots](/screenshots) below.

17. The more datasets you load, the more categories you can see. That's why I offer you to download and process at least 2 datasets. You can process 3rd dataset by following familiar steps in [Ingest data using CLI (bash & python)](#keyboard-ingest-data-using-cli-bash--python) or go ahead and discover [Mage AI](#mage-orchestrate-data-ingestion-mage-ai).

### :mage: Orchestrate data ingestion (Mage AI)

I wouldn't say it was so simple and easy as Matt showed us in videos, but with some time and effort I managed to find the way to convert the logic of my `jsonl_postgres.py` script into `Mage` pipelines and bricks. Why is it worth my/your time? Because larger datasets (like Kindle_Store) will probably demand a serious cloud storage, database and compute than free CodeSpace playground. And I (you?) need to learn how to deal with them with a more scalable system, providing long job execution, partitioning, monitoring and logs. So let's see 2 pipelines I managed to setup with Mage. 

💡 In case something goes wrong or complicated, you can still ingest more datasets with step 11 [Ingest data using CLI (bash & python)](#keyboard-ingest-data-using-cli-bash--python) and then reload Metabase dashboard page.

18. Run `unzip mage.zip` to extract pre-configured Mage AI workflow.
```bash
unzip mage.zip
```
19. Run `docker-compose build` to automatically download and prepare Docker container with Mage AI.
```bash
docker-compose build
```
20. Run `docker-compose up` to start Docker container with Mage AI.
```bash
docker-compose up
```
21. CodeSpace will pop-up the notification that `Your application running on port 6789 is available.` - click `Open in Browser`.

![Open Mage AI page](/screenshots/open-mage-ai-page.png)

That new page would probably open white. Please wait a couple of seconds to let Mage AI start, then refresh the page. Now you will see Mage dashboard. Your orchestration center is ready to serve you! ✅

22. Go to `Pipelines` - move mouse to the left edge of the window, it will open menu slider, click on `Pipelines`.

![Open Mage AI pipelines](/screenshots/mage-pipelines.png)

You will see 2 pipelines I configured: 

![Mage AI pipelines list](/screenshots/mage-pipelines-list.png)

23. Click `load_run_dataset`. It will open current pipeline triggers. Then click `Run@once` button.

![Mage AI pipeline trigger](/screenshots/mage-run-pipeline-trigger.png)

24. In the `Run pipeline now` dialogue you can define a variable, in this case it will be the file name of dataset.  

![Mage AI pipeline run variable](/screenshots/mage-run-pipeline-variable.png)

Enter `dataset_urls2.csv` (that you downloaded but not processed yet) and click `Run now` button. Ingestion process started. You can see the progress on `Pipeline runs` page.

![Mage AI pipeline run progress](/screenshots/mage-ai-pipeline-runs-running.png)

25. After ingesting new dataset you can switch to Metabase page, update it and see new data in the dashboard reports. Congratulations! ✅🎉

![Metabase dashboard products](/screenshots/metabase-dashboard-products.png)

![Metabase dashboard reviews by verified monthly](/screenshots/reviews-by-verified-purchase-monthly.png)

![Metabase dashboard reviews by category monthly](/screenshots/reviews-by-main-category-monthly.png)

26. If you get to this point, please ⭐️star⭐️ my repo! 🙌

## :stop_button: Instructions to stop the apps

- Simple way - stop all together by stopping your CodeSpace. Remember, this will leave all downloaded data in your CodeSpace - you can start it later and continue playing with tools and data. You can also delete CodeSpace with all the data.
- Stop all active Docker containers - run this command in terminal
```bash
docker stop $(docker ps -a -q)
```
- Stop Mage - switch to 2nd terminal and press `Ctrl-C`
- Stop PostgreSQL - switch to 1st terminal and press `Ctrl-C`

## Support

🙏 Thank you for your attention and time!

- If you experience any issue while following this instruction (or something left unclear), please add it to [Issues](https://github.com/dmytrovoytko/data-engineering-amazon-reviews/issues), I'll be glad to help/fix. And your feedback, questions & suggestions are welcome as well!
- Feel free to fork and submit pull requests.

If you find this project helpful, please ⭐️star⭐️ my repo https://github.com/dmytrovoytko/data-engineering-amazon-reviews to help other people discover it 🙏

Made with ❤️ in Ukraine 🇺🇦 Dmytro Voytko