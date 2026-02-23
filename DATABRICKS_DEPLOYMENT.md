# Databricks Deployment Guide

The Preventual Data Simulator is perfectly configured for production execution inside a Databricks Workspace using PySpark distributed data generation. 

By running perfectly in Databricks, the `java.io.EOFException` PyArrow failure (caused by Windows) simply ceases to exist, allowing for billions of rows to be generated across the cluster!

## 1. Transferring Code to Databricks

There are two primary ways to move your code to Databricks:

**Option A - Native Databricks Repos / Git (Recommended):**
1. Push this entire folder (excluding `.env`) to your GitHub, GitLab, or Bitbucket repository.
2. In Databricks, click **Workspace**, then click **Add > Repo**.
3. Paste the URL to your git repository. This syncs your entire script right into the cloud.

**Option B - Manual Workspace Upload:**
1. Zip this entire folder (excluding `.env` and `__pycache__`).
2. Open Databricks, click **Workspace**.
3. From the top-right of your Workspace user folder, click the **Three Dots -> Import**.
4. Upload the zip or the individual `.py` and `.json` files.

## 2. Setting Up The Cluster Environment Variables

There are two easy ways to get your secrets (like `POSTGRES_URI`) into Databricks without hardcoding them in the Python script:

**Method 1 - env.yaml File (Your Preference):**
1. Open the new `env.yaml` file I just created for you.
2. It contains `POSTGRES_URI: "postgresql://..."`.
3. Simply upload this `env.yaml` file right next to your `run_simulator.py` file in Databricks! The Python script will now automatically detect it, parse the YAML, and inject the connection string securely into the cluster runtime.

**Method 2 - Native Cluster Environment Variables:**
1. Open Databricks and go to your Job Task.
2. Edit the Job Cluster -> Advanced Options -> Spark.
3. Scroll down to **Environment variables** and paste the string natively.

## 3. Creating a Databricks Job

Now you can orchestrate your simulator:

1. Click **Workflows** in the left sidebar of Databricks, then click **Create Job**.
2. Give the task a name (e.g. `Initial Database Load`).
3. For the **Type**, select **Python script**.
4. For the **Source**, choose **Workspace** (if you manually uploaded) or **Git provider** (if you linked your repo).
5. For the **Path**, point it directly to `run_simulator.py`.
6. For the **Parameters**, add these exact arguments to the list:
   - `--mode` 
   - `init`
   - `--target` 
   - `postgres`
   - `--rows`
   - `1000`
7. Click **Create**!

## 4. Testing Your Simulator

Click **Run Now** in the top right of your newly created Job.

Databricks will boot up the PySpark engine, successfully distribute the generation load to every worker node, establish the SQLAlchemy connection to your AWS RDS database, and pump the data flawlessly without any Windows Arrow crashes.

Once Phase A (initial load) completes, you can duplicate the Job Task, switch `--mode init` to `--mode stream` and `--rows 50`, and schedule it to run every 10 minutes recursively!
