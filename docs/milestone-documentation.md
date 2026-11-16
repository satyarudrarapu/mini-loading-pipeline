# Mini Loading Pipeline Project — Complete Setup Documentation

**Project:** USGS Earthquake Data Pipeline
**Author:** [Your Name]
**Date:** November 2026
**Purpose:** Step-by-step guide for setting up the complete pipeline from scratch.
Written for someone with no prior AWS or Python experience.

---

## Table of Contents

1. What This Project Does
2. Tools You Need Before Starting
3. AWS Account Setup
4. AWS Security Setup (IAM)
5. AWS Budget Alert
6. S3 Bucket Setup
7. Project Code Setup (Local Machine)
8. AWS CLI Setup and Login
9. Virtual Environment and Dependencies
10. Deploy Milestone 2 — Ingest Stage
11. Run the Backfill
12. Deploy Milestone 3 — Sort Stage
13. How to Test Everything
14. How to Verify in the AWS Console
15. Architecture Diagram
16. How to Stop and Start the Pipeline
17. Troubleshooting
18. Commands Cheat Sheet
19. Screenshot Checklist

---

## Section 1 — What This Project Does

This project builds an automated data pipeline that:

1. Automatically downloads earthquake data from USGS (US Geological Survey) every 15 minutes
2. Saves each file to Amazon S3 (cloud storage)
3. Sorts the files by table name and date into an organised structure
4. Sends messages to a load queue ready for Milestone 4 (Spark + Snowflake)

**Data source:** https://earthquake.usgs.gov — free, no API key needed, updates every minute.

**Flow:**
```
Every 15 minutes (automatic, no laptop needed):

EventBridge Scheduler → ECS Fargate (ingest) → S3 raw/
                                                      ↓
                                              S3 event notification
                                                      ↓
                                              SQS sort queue
                                                      ↓
                                      ECS Fargate (sort worker) → S3 sorted/
                                                                        ↓
                                                               SQS load queue
                                                            (Milestone 4 → Snowflake)

Manual (run once to load history):

Backfill Script → USGS History API → S3 raw/history/
```

**AWS Services used:**

| Service | What it does in this project |
|---|---|
| S3 | Stores all earthquake data files |
| ECS Fargate | Runs the Python code for ingest and sort |
| ECR | Stores the Docker container image |
| EventBridge Scheduler | Triggers ingest every 15 minutes |
| SQS | Queues work between stages |
| CloudWatch | Stores all logs and raises alarms |
| IAM | Controls who can do what in AWS |

---

## Section 2 — Tools You Need Before Starting

Install each tool in this order. Take a screenshot after each successful installation.

---

### 2.1 Python 3.12

**What it is:** The programming language the project is written in.

**How to install:**
1. Go to https://www.python.org/downloads/
2. Click the yellow **Download Python 3.12.x** button
3. Run the installer
4. **IMPORTANT:** Tick the checkbox **"Add Python to PATH"** before clicking Install
5. Click **Install Now**

**Verify installation — open PowerShell and run:**
```powershell
python --version
```
Expected output: `Python 3.12.x`

> 📸 SCREENSHOT: Take a screenshot of PowerShell showing `Python 3.12.x`

---

### 2.2 Docker Desktop

**What it is:** Runs containers — isolated environments for your code. ECS Fargate uses Docker images.

**How to install:**
1. Go to https://www.docker.com/products/docker-desktop/
2. Click **Download Docker Desktop for Windows**
3. Run the installer, follow all prompts
4. Restart your computer when asked
5. Open Docker Desktop from the Start menu
6. Wait for the whale icon in the taskbar to stop animating — that means it is ready

**Verify installation:**
```powershell
docker --version
```
Expected output: `Docker version 29.x.x`

> 📸 SCREENSHOT: Take a screenshot of Docker Desktop showing it is running (whale icon in taskbar)

---

### 2.3 AWS CLI v2

**What it is:** A command-line tool that lets you control AWS from your terminal.

**How to install:**
1. Open PowerShell and run:
```powershell
irm 'https://awscli.amazonaws.com/v2/install.ps1' | iex
```
2. Close and reopen PowerShell after installation

**Verify installation:**
```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" --version
```
Expected output: `aws-cli/2.x.x`

> 📸 SCREENSHOT: Take a screenshot of PowerShell showing the AWS CLI version

---

### 2.4 Git

**What it is:** Version control — tracks changes to your code and lets you push to GitHub.

**How to install:**
1. Go to https://git-scm.com/download/win
2. Download and run the installer
3. Keep all default settings throughout

**Verify installation:**
```powershell
git --version
```
Expected output: `git version 2.x.x`

> 📸 SCREENSHOT: Take a screenshot showing `git version 2.x.x`

---

### 2.5 Visual Studio Code

**What it is:** A code editor to view and edit the Python files.

**How to install:**
1. Go to https://code.visualstudio.com/
2. Click **Download for Windows**
3. Run the installer, keep all defaults

> 📸 SCREENSHOT: Take a screenshot of VS Code open with the USGS project folder

---

## Section 3 — AWS Account Setup

### 3.1 Create an AWS account

> If you already have an AWS account, skip to Section 3.2.

1. Go to https://aws.amazon.com
2. Click **Create an AWS Account** (top right)
3. Enter your email address → click **Verify email address**
4. Check your email for a verification code — enter it
5. Create a password
6. Choose **Personal** account type
7. Enter your contact information
8. Enter payment details (credit card required — Free Tier keeps costs near zero)
9. Verify your phone number
10. Choose the **Free** support plan
11. Click **Complete sign up**
12. Sign in at https://console.aws.amazon.com

> 📸 SCREENSHOT: Take a screenshot of the AWS Console home page after signing in

---

### 3.2 Set your region

> All resources in this project must be in **ap-southeast-2 (Sydney)**.
> Every time you open the console, check this first.

1. Sign in to https://console.aws.amazon.com
2. Look at the top-right corner — you see a region name (e.g. "N. Virginia")
3. Click it
4. Find and click **Asia Pacific (Sydney) ap-southeast-2**

> 📸 SCREENSHOT: Take a screenshot showing ap-southeast-2 selected in the top-right corner

---

### 3.3 Enable MFA (Multi-Factor Authentication)

> Security requirement from the project document — do not skip this.

1. Click your account name (top right) → **Security credentials**
2. Scroll down to **Multi-factor authentication (MFA)**
3. Click **Assign MFA device**
4. Choose **Authenticator app** → click Next
5. Install **Google Authenticator** on your phone
6. Open Google Authenticator → tap **+** → scan the QR code
7. Enter two consecutive 6-digit codes from the app
8. Click **Add MFA**

> 📸 SCREENSHOT: Take a screenshot of the MFA section showing your device is active

---

## Section 4 — AWS Security Setup (IAM)

### 4.1 Create the S3 access policy

1. AWS Console → search bar → type **IAM** → click IAM
2. Click **Policies** in the left menu
3. Click **Create policy** (top right)
4. Click the **JSON** tab
5. Delete all existing text in the editor
6. Paste this exactly:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListProjectBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::mini-loading-pipeline-bucket",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["raw/*", "sorted/*"]
        }
      }
    },
    {
      "Sid": "AccessProjectObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:GetObjectTagging",
        "s3:PutObjectTagging"
      ],
      "Resource": [
        "arn:aws:s3:::mini-loading-pipeline-bucket/raw/*",
        "arn:aws:s3:::mini-loading-pipeline-bucket/sorted/*"
      ]
    }
  ]
}
```

7. Click **Next**
8. Policy name: `MiniLoadingPipeline-S3-Access`
9. Click **Create policy**

> 📸 SCREENSHOT: Take a screenshot of the policy showing its name and the JSON editor

---

### 4.2 Create an IAM user

1. IAM → **Users** (left menu) → **Create user**
2. User name: `mini-loading-pipeline-user`
3. Click **Next**
4. Select **Attach policies directly**
5. Search for `MiniLoadingPipeline-S3-Access` → tick the checkbox
6. Click **Next** → **Create user**

> 📸 SCREENSHOT: Take a screenshot of the user page showing the attached policy

---

## Section 5 — AWS Budget Alert

> Do this before creating any AWS resources. Project requirement.

1. AWS Console → search **Budgets** → click **AWS Budgets**
2. Click **Create budget**
3. Choose **Use a template** → **Monthly cost budget**
4. Budget name: `mini-loading-pipeline-budget`
5. Enter a monthly limit — for example **$10.00**
6. Enter your email address for alerts
7. Click **Create budget**

> 📸 SCREENSHOT: Take a screenshot of the created budget showing the monthly limit

---

## Section 6 — S3 Bucket Setup

### 6.1 Create the bucket

1. AWS Console → search **S3** → click S3
2. Click **Create bucket**
3. Bucket name: `mini-loading-pipeline-bucket`
4. Region: **ap-southeast-2**
5. Leave **Block all public access** turned ON (default)
6. Click **Create bucket**

> 📸 SCREENSHOT: Take a screenshot of the bucket appearing in the S3 bucket list

---

### 6.2 Create the folder structure

1. Click the bucket name `mini-loading-pipeline-bucket`
2. Click **Create folder** → Name: `raw` → click **Create folder**
3. Click into `raw/` → **Create folder** → Name: `usgs` → **Create folder**
4. Click into `usgs/` → **Create folder** → Name: `all_day` → **Create folder**
5. Go back to `usgs/` → **Create folder** → Name: `history` → **Create folder**
6. Go back to bucket root → **Create folder** → Name: `sorted` → **Create folder**

Final structure:
```
mini-loading-pipeline-bucket/
├── raw/
│   └── usgs/
│       ├── all_day/      ← live feed files land here every 15 min
│       └── history/      ← backfill files land here
└── sorted/               ← sort worker organises files here
```

> 📸 SCREENSHOT: Take a screenshot of the bucket showing all folders

---

## Section 7 — Project Code Setup (Local Machine)

### 7.1 Get the code

1. Open PowerShell
2. Navigate to your Desktop:
```powershell
cd "C:\Users\savenlaptop-45\OneDrive\Desktop"
```
3. Clone the repository:
```powershell
git clone https://github.com/<your-github-username>/USGS.git
```
4. Navigate into the project:
```powershell
cd USGS
```

> 📸 SCREENSHOT: Take a screenshot of the USGS folder in File Explorer

---

### 7.2 Project structure explained

Open the folder in VS Code. You will see:

```
USGS/
├── Dockerfile              ← defines the Docker container for ECS
├── .gitignore              ← tells git what not to commit
├── requirements.txt        ← Python packages needed
├── pipeline/               ← code that runs in AWS (ECS containers)
│   ├── settings.py         ← all configuration in one place
│   ├── ingest/
│   │   ├── worker.py       ← ECS ingest worker (fetches USGS → S3)
│   │   └── backfill.py     ← historical backfill script
│   └── sort/
│       ├── rules.py        ← Pydantic sort rules (table + date mapping)
│       └── worker.py       ← ECS sort worker (raw/ → sorted/)
└── scripts/                ← tools that run on your laptop only
    ├── load_creds.py       ← loads AWS credentials for boto3
    ├── deploy.py           ← deploys ingest stage to AWS
    ├── deploy_sort.py      ← deploys sort stage to AWS
    ├── test_ingest.py      ← tests the ingest pipeline
    ├── test_sort.py        ← tests the sort pipeline
    ├── run_backfill.py     ← runs the historical backfill
    └── reconcile.py        ← checks no files were silently lost
```

**Key rule:**
- `pipeline/` → deployed to AWS (runs inside ECS Docker containers)
- `scripts/` → only ever runs on your laptop

> 📸 SCREENSHOT: Take a screenshot of VS Code showing this folder structure in the sidebar

---

## Section 8 — AWS CLI Setup and Login

### 8.1 Log in to AWS from the terminal

```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" configure set region ap-southeast-2 --profile default
```

Then log in:
```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" login
```

A browser window opens. Sign in with your Google or GitHub account. When the page says **"Authentication successful"**, come back to the terminal.

> 📸 SCREENSHOT: Take a screenshot of the terminal after the login success message

---

### 8.2 Verify the session is working

```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" sts get-caller-identity --profile default
```

Expected output:
```json
{
    "UserId": "...",
    "Account": "106217160836",
    "Arn": "arn:aws:sts::106217160836:assumed-role/AccountFullAccessRole/..."
}
```

If you see your Account ID, the session is active.

> 📸 SCREENSHOT: Take a screenshot showing your Account ID in the output

> **Important:** Your session expires every 12 hours.
> When it does, run `aws login` again to renew it.

---

## Section 9 — Virtual Environment and Dependencies

### 9.1 Create the virtual environment

```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Python\pythoncore-3.12-64\python.exe" -m venv .venv
```

> 📸 SCREENSHOT: Take a screenshot of the .venv folder appearing in File Explorer

---

### 9.2 Install pip inside the virtual environment

```powershell
.venv\Scripts\python.exe -m ensurepip --upgrade
```

---

### 9.3 Install all project dependencies

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

This installs:

| Package | What it does |
|---|---|
| boto3 | AWS SDK for Python — talks to S3, ECS, SQS, CloudWatch |
| requests | HTTP library — downloads data from USGS API |
| pydantic | Data validation — used for sort rules typed config |
| pytest | Test runner |
| pytest-mock | Mocking for tests |

> 📸 SCREENSHOT: Take a screenshot of the terminal showing all packages installed

---

## Section 10 — Deploy Milestone 2 — Ingest Stage

### 10.1 Make sure Docker Desktop is running

Look at the Windows taskbar — the whale icon should be white and not animating.

> 📸 SCREENSHOT: Take a screenshot of Docker Desktop showing it is running

---

### 10.2 Run the deployment script

```powershell
.venv\Scripts\python.exe scripts/deploy.py
```

You will see this output step by step:

```
============================================================
Mini Loading Pipeline — Ingest Deployment
Architecture: EventBridge Scheduler → ECS Fargate (no Lambda)
============================================================
[1] ECR repository — Created
[2] Build Docker image — Image built
[3] Push image to ECR — Pushed
[4] ECS cluster — Created: mini-loading-pipeline-cluster
[5] CloudWatch log group — Created: /ecs/mini-loading-pipeline-ingest
[6] IAM roles — Created task role + execution role
[7] ECS task definition — Registered: mini-loading-pipeline-ingest-task
[8] VPC configuration — Found default VPC, subnet, security group
[9] IAM role for EventBridge — Created
[10] EventBridge Scheduler — Schedule created (rate: 15 minutes)
Deployment complete.
```

> 📸 SCREENSHOT: Take a screenshot of the full deployment output

---

### 10.3 Verify ingest deployment in the AWS Console

| Resource | Where to find it |
|---|---|
| ECR repository | ECR → Repositories → `mini-loading-pipeline-ingest` |
| ECS cluster | ECS → Clusters → `mini-loading-pipeline-cluster` |
| Ingest task def | ECS → Task definitions → `mini-loading-pipeline-ingest-task` |
| EventBridge schedule | EventBridge → Schedules → `mini-loading-pipeline-ingest-schedule` |
| CW log group | CloudWatch → Log groups → `/ecs/mini-loading-pipeline-ingest` |

> 📸 SCREENSHOT: Take a screenshot of the EventBridge Schedules page showing the schedule is Enabled

---

## Section 11 — Run the Backfill (F2)

This downloads 7 days of historical earthquake data into S3. Run once — safe to run again (idempotent).

```powershell
.venv\Scripts\python.exe scripts/run_backfill.py --start 2026-11-01 --end 2026-11-07
```

Expected output:
```
F2 PASSED: 7/7 files in S3.
```

**Run the same command again to prove idempotency (N1):**
```powershell
.venv\Scripts\python.exe scripts/run_backfill.py --start 2026-11-01 --end 2026-11-07
```

Expected: `Files uploaded this run : 0` — nothing changed on second run.

> 📸 SCREENSHOT: Take a screenshot of both outputs — first run (7 uploads) and second run (0 uploads)

---

## Section 12 — Deploy Milestone 3 — Sort Stage

```powershell
.venv\Scripts\python.exe scripts/deploy_sort.py
```

Expected output:
```
[1] SQS queues — sort DLQ, sort queue, load queue created
[2] SQS queue policy — S3 allowed to publish
[3] S3 event notification — raw/ → sort queue configured
[4] CloudWatch log group — /ecs/mini-loading-pipeline-sort created
[5] ECS sort task definition — mini-loading-pipeline-sort-task registered
[6] IAM inline policy — SQS access on ECS task role
[7] Sort config verified
[8] CloudWatch DLQ alarm — mini-loading-pipeline-sort-dlq-alarm created
Milestone 3 deployment complete.
```

> 📸 SCREENSHOT: Take a screenshot of the full deploy_sort.py output

---

## Section 13 — How to Test Everything

### Test 1 — Ingest worker code (F1 — fastest local test)

```powershell
.venv\Scripts\python.exe scripts/test_ingest.py --worker
```

Expected: `F1 PASSED.`

> 📸 SCREENSHOT: Take a screenshot of F1 PASSED output

---

### Test 2 — Sort rules unit tests (N6 — no AWS needed)

```powershell
.venv\Scripts\python.exe -m pytest tests/test_sort_rules.py -v
```

Expected: `21 passed in 0.32s`

> 📸 SCREENSHOT: Take a screenshot of 21 passed

---

### Test 3 — Full ingest in AWS (F1 — ECS test)

```powershell
.venv\Scripts\python.exe scripts/test_ingest.py --invoke
```

Expected: ECS task exits 0, `F1 PASSED.`

> 📸 SCREENSHOT: Take a screenshot of the full invoke output

---

### Test 4 — Sort pipeline (F3, F4, F6)

**Terminal 1:**
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --run-worker
```

**Terminal 2:**
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --test-f3-f6
```

Expected: `=== F3 PASS, F4 PASS, F6 PASS ===`

> 📸 SCREENSHOT: Take a screenshot of F3 F4 F6 all PASSED

---

### Test 5 — DLQ (F5)

```powershell
.venv\Scripts\python.exe scripts/test_sort.py --test-f5
```

Expected: `=== F5 PASS ===`

> 📸 SCREENSHOT: Take a screenshot of F5 PASSED

---

### Test 6 — No silent loss (N3)

```powershell
.venv\Scripts\python.exe scripts/test_sort.py --test-n3
```

Expected: `N3 PASSED: no silent loss detected.`

> 📸 SCREENSHOT: Take a screenshot of N3 PASSED with 0 missing

---

### Test 7 — Sort worker on ECS (production method)

```powershell
.venv\Scripts\python.exe scripts/test_sort.py --start-ecs
```

Expected: Sort worker ECS task started, Task ID shown.

> 📸 SCREENSHOT: Take a screenshot of the ECS task started output

**Stop it after testing:**
```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" ecs stop-task --cluster mini-loading-pipeline-cluster --task <TASK-ID-FROM-ABOVE> --region ap-southeast-2 --profile default
```

---

## Section 14 — How to Verify in the AWS Console

### 14.1 S3 files are growing automatically (F1)

**Console:** S3 → `mini-loading-pipeline-bucket` → `raw/usgs/all_day/` → today's date folder

New `.geojson` files appear every 15 minutes automatically.

> 📸 SCREENSHOT: Take a screenshot showing files with timestamps 15 minutes apart

---

### 14.2 EventBridge Scheduler (senior's change — no Lambda)

**Console:** EventBridge → **Schedules** (left menu) → `mini-loading-pipeline-ingest-schedule`

You will see:
- **Schedule:** `rate(15 minutes)`
- **State:** Enabled
- **Target:** Amazon ECS cluster (not Lambda — confirms senior's change)

> 📸 SCREENSHOT: Take a screenshot of the schedule showing ECS as target

---

### 14.3 ECS tasks ran successfully

**Console:** ECS → Clusters → `mini-loading-pipeline-cluster` → Tasks tab

Click any stopped task → check **Exit code: 0**

> 📸 SCREENSHOT: Take a screenshot of a task with exit code 0

---

### 14.4 CloudWatch logs show JSON events (F10)

**Console:** CloudWatch → Log groups → `/ecs/mini-loading-pipeline-ingest` → latest stream

You will see:
```json
{"event": "file_ingested", "table": "usgs.earthquakes", "key": "raw/usgs/all_day/...", "byte_count": 148360}
```

Also check `/ecs/mini-loading-pipeline-sort`:
```json
{"event": "object_sorted", "table": "usgs.earthquakes", "data_date": "2026-11-12", ...}
```

> 📸 SCREENSHOT: Take a screenshot of the JSON log lines in CloudWatch

---

### 14.5 SQS queues (F3, F5, F6)

**Console:** SQS → Queues

| Queue | What to show |
|---|---|
| `mini-loading-pipeline-sort` | Visible = 0 when worker running |
| `mini-loading-pipeline-sort-dlq` | Should be 0 after cleanup |
| `mini-loading-pipeline-load` | 100+ messages waiting for Milestone 4 |

Click `mini-loading-pipeline-sort` → Details → **Dead-letter queue: mini-loading-pipeline-sort-dlq, Max receives: 5** — this proves F5.

> 📸 SCREENSHOT: Take a screenshot of all three queues

---

### 14.6 S3 event notification (F3)

**Console:** S3 → `mini-loading-pipeline-bucket` → **Properties** tab → scroll to **Event notifications**

You will see:
```
Name: RawObjectCreatedToSortQueue
Events: s3:ObjectCreated:*
Prefix: raw/
Destination: SQS — mini-loading-pipeline-sort
```

> 📸 SCREENSHOT: Take a screenshot of the event notification configuration

---

### 14.7 sorted=true tag on raw files (F4)

**Console:** S3 → `raw/usgs/all_day/` → click any `.geojson` file → **Properties** tab → scroll to **Tags**

You will see: `sorted = true`

> 📸 SCREENSHOT: Take a screenshot showing sorted=true tag

---

### 14.8 CloudWatch DLQ alarm (N2)

**Console:** CloudWatch → Alarms → `mini-loading-pipeline-sort-dlq-alarm`

- **State: OK** = healthy, DLQ is empty
- **State: ALARM** = something failed, check sort logs

> 📸 SCREENSHOT: Take a screenshot of the alarm showing OK state

---

### 14.9 IAM roles (security)

**Console:** IAM → Roles → search `mini-loading-pipeline`

You will see three roles and NO Lambda role (confirms senior's change):
- `mini-loading-pipeline-ecs-task-role` — S3 + SQS + CloudWatch
- `mini-loading-pipeline-ecs-execution-role` — ECR pull
- `mini-loading-pipeline-eventbridge-role` — ECS RunTask

> 📸 SCREENSHOT: Take a screenshot of the roles list showing all three and no Lambda role

---

## Section 15 — Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│  AWS Cloud (ap-southeast-2 / Sydney)                                 │
│                                                                      │
│  ┌─────────────────┐  fires every 15 min  ┌──────────────────────┐  │
│  │  EventBridge    │─────────────────────►│  ECS Fargate         │  │
│  │  Scheduler      │  (direct — no Lambda)│  pipeline/ingest/    │  │
│  │  rate(15 min)   │                      │  worker.py           │  │
│  └─────────────────┘                      │                      │  │
│                                           │  1. fetch USGS API   │  │
│                                           │  2. upload to S3     │  │
│                                           │  3. write JSON log   │  │
│                                           └──────────┬───────────┘  │
│                                                      │              │
│  S3: raw/usgs/all_day/YYYY-MM-DD/HHMMSS.geojson ◄───┘              │
│       │                                                             │
│       │  S3 event notification fires automatically                  │
│       ▼                                                             │
│  SQS sort queue (1 message per file)                                │
│       │                                                             │
│       │  long-poll                                                  │
│       ▼                                                             │
│  ECS Fargate (sort worker — long-running)                           │
│  pipeline/sort/worker.py                                            │
│       │  match key → table + data_date                              │
│       │  copy raw/ → sorted/usgs.earthquakes/YYYY-MM-DD/            │
│       │  tag raw with sorted=true                                   │
│       │  send to load queue                                         │
│       │  delete sort message                                        │
│       ▼                                                             │
│  S3: sorted/usgs.earthquakes/YYYY-MM-DD/HHMMSS.geojson             │
│  SQS load queue → Milestone 4 (Spark + Snowflake)                  │
│                                                                     │
│  CloudWatch logs all events. DLQ alarm fires on failure (N2).      │
└──────────────────────────────────────────────────────────────────────┘

Your Laptop (not part of AWS):
  scripts/deploy.py        → one-time setup of ingest stage
  scripts/deploy_sort.py   → one-time setup of sort stage
  scripts/test_ingest.py   → verify ingest works
  scripts/test_sort.py     → verify sort works
  scripts/run_backfill.py  → download historical data (once)
  scripts/reconcile.py     → check no silent data loss
```

---

## Section 16 — How to Stop and Start the Pipeline

### Stop everything (save AWS credits)

**Step 1 — Stop the sort worker ECS task:**
```powershell
.venv\Scripts\python.exe -c "
import sys, boto3
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from scripts.load_creds import load, load_config
load(); load_config()
from pipeline import settings
ecs = boto3.Session(region_name=settings.AWS_REGION).client('ecs')
tasks = ecs.list_tasks(cluster=settings.ECS_CLUSTER_NAME, desiredStatus='RUNNING')
for arn in tasks['taskArns']:
    ecs.stop_task(cluster=settings.ECS_CLUSTER_NAME, task=arn)
    print('Stopped:', arn.split('/')[-1])
print('Done.' if tasks['taskArns'] else 'No running tasks.')
"
```

**Step 2 — Disable EventBridge Scheduler:**

**In the Console:** EventBridge → Schedules → `mini-loading-pipeline-ingest-schedule` → Edit → Change State to **Disabled** → Save

**Or in PowerShell:**
```powershell
.venv\Scripts\python.exe -c "
import sys, boto3
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from scripts.load_creds import load, load_config
load(); load_config()
from pipeline import settings
scheduler = boto3.Session(region_name=settings.AWS_REGION).client('scheduler')
sched = scheduler.get_schedule(Name='mini-loading-pipeline-ingest-schedule')
scheduler.update_schedule(
    Name='mini-loading-pipeline-ingest-schedule',
    ScheduleExpression=sched['ScheduleExpression'],
    FlexibleTimeWindow=sched['FlexibleTimeWindow'],
    Target=sched['Target'],
    State='DISABLED',
)
print('EventBridge Scheduler DISABLED.')
"
```

---

### Start everything again

**Step 1 — Enable EventBridge Scheduler:**

**In the Console:** EventBridge → Schedules → `mini-loading-pipeline-ingest-schedule` → Edit → Change State to **Enabled** → Save

**Step 2 — Start sort worker on ECS:**
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --start-ecs
```

---

## Section 17 — Troubleshooting

### "SESSION EXPIRED — run aws login first"

**Cause:** Your 12-hour AWS session expired.

**Fix:**
```powershell
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" login
```

---

### "ERROR: Docker is not running"

**Cause:** Docker Desktop is closed.

**Fix:** Open Docker Desktop from the Start menu. Wait for the whale icon to stop animating. Then re-run your command.

---

### Sort queue messages not being processed

**Cause:** The sort worker is not running.

**Fix:** Start it on your laptop:
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --run-worker
```
Or on ECS:
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --start-ecs
```

---

### N3 shows missing objects

**Cause:** New files arrived from EventBridge but the sort worker was not running.

**Fix:** Start the sort worker and wait for it to drain the queue, then run N3 again.
```powershell
.venv\Scripts\python.exe scripts/test_sort.py --run-worker
# Wait for messages to be processed, then Ctrl+C
.venv\Scripts\python.exe scripts/test_sort.py --test-n3
```

---

### DLQ alarm is in ALARM state

**Cause:** Messages went to the DLQ — either from the s3:TestEvent AWS sends automatically, or from F5 test files.

**Fix:** This is expected during testing. Purge the DLQ:
```powershell
.venv\Scripts\python.exe -c "
import sys, boto3
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
from scripts.load_creds import load, load_config
load(); load_config()
from pipeline import settings
sqs = boto3.Session(region_name=settings.AWS_REGION).client('sqs')
sqs.purge_queue(QueueUrl=settings.SQS_SORT_DLQ_URL)
print('DLQ purged.')
"
```

---

## Section 18 — Commands Cheat Sheet

### Session management
```powershell
# Check session
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" sts get-caller-identity --profile default

# Renew session
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" login
```

### Testing
```powershell
# F1 locally
.venv\Scripts\python.exe scripts/test_ingest.py --worker

# F1 in AWS
.venv\Scripts\python.exe scripts/test_ingest.py --invoke

# F2 + N1
.venv\Scripts\python.exe scripts/run_backfill.py --start 2026-11-01 --end 2026-11-07

# F3 F4 F6 (run --run-worker in Terminal 1 first)
.venv\Scripts\python.exe scripts/test_sort.py --test-f3-f6

# F5
.venv\Scripts\python.exe scripts/test_sort.py --test-f5

# N3
.venv\Scripts\python.exe scripts/test_sort.py --test-n3

# N6
.venv\Scripts\python.exe -m pytest tests/test_sort_rules.py -v
```

### Deployment
```powershell
# Deploy ingest stage (Milestone 2)
.venv\Scripts\python.exe scripts/deploy.py

# Deploy sort stage (Milestone 3)
.venv\Scripts\python.exe scripts/deploy_sort.py
```

### Sort worker
```powershell
# Run locally on laptop
.venv\Scripts\python.exe scripts/test_sort.py --run-worker

# Run as ECS Fargate task (production)
.venv\Scripts\python.exe scripts/test_sort.py --start-ecs

# Stop ECS task
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" ecs stop-task --cluster mini-loading-pipeline-cluster --task <TASK-ID> --region ap-southeast-2 --profile default
```

### Check S3 files
```powershell
# Live feed files
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" s3 ls "s3://mini-loading-pipeline-bucket/raw/usgs/all_day/" --recursive --region ap-southeast-2 --profile default

# History files
& "C:\Users\savenlaptop-45\AppData\Local\Programs\Amazon\AWSCLIV2\aws.exe" s3 ls "s3://mini-loading-pipeline-bucket/raw/usgs/history/" --region ap-southeast-2 --profile default
```

---

## Section 19 — Screenshot Checklist

Use this to make sure you have captured every required screenshot.

- [ ] 2.1 — Python 3.12 version in PowerShell
- [ ] 2.2 — Docker Desktop running (whale icon in taskbar)
- [ ] 2.3 — AWS CLI version in PowerShell
- [ ] 2.4 — Git version in PowerShell
- [ ] 2.5 — VS Code open with USGS project
- [ ] 3.1 — AWS Console home after sign in
- [ ] 3.2 — Region selector showing ap-southeast-2
- [ ] 3.3 — MFA device active in IAM
- [ ] 4.1 — MiniLoadingPipeline-S3-Access policy with JSON
- [ ] 4.2 — IAM user with policy attached
- [ ] 5   — AWS Budget with limit and email
- [ ] 6.1 — S3 bucket in bucket list
- [ ] 6.2 — S3 bucket with all folders visible
- [ ] 7.1 — USGS folder in File Explorer
- [ ] 7.2 — VS Code showing project folder structure
- [ ] 8.1 — Terminal showing login succeeded
- [ ] 8.2 — Terminal showing Account ID from sts get-caller-identity
- [ ] 9.3 — Packages installed successfully
- [ ] 10.1 — Docker Desktop running
- [ ] 10.2 — Full deploy.py output in terminal
- [ ] 10.3 — EventBridge Schedules page showing schedule Enabled
- [ ] 11   — F2 PASSED output (7 uploads first run)
- [ ] 11   — F2 PASSED output (0 uploads second run — idempotency proof)
- [ ] 12   — Full deploy_sort.py output
- [ ] 13.1 — F1 PASSED — worker mode
- [ ] 13.2 — 21 passed pytest output
- [ ] 13.3 — F1 PASSED — invoke mode
- [ ] 13.4 — F3 F4 F6 all PASSED
- [ ] 13.5 — F5 PASSED
- [ ] 13.6 — N3 PASSED 0 missing
- [ ] 13.7 — Sort worker ECS task started
- [ ] 14.1 — S3 files 15 minutes apart
- [ ] 14.2 — EventBridge schedule showing ECS as target (not Lambda)
- [ ] 14.3 — ECS task with exit code 0
- [ ] 14.4 — CloudWatch JSON log events (ingest + sort)
- [ ] 14.5 — All three SQS queues
- [ ] 14.6 — S3 event notification configuration
- [ ] 14.7 — sorted=true tag on a raw file
- [ ] 14.8 — CloudWatch DLQ alarm OK state
- [ ] 14.9 — IAM roles list (3 roles, no Lambda role)

---

*Document prepared by [Your Name] — November 2026*
*Replace `savenlaptop-45` with your actual Windows username in all commands if different*
