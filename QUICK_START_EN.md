# ⚡ Quick Start Guide (English Version)

> **Estimated Time**: 30 minutes  
> **Difficulty**: ⭐ Simple  
> **Prerequisites**: Python 3.11+, Prefect Cloud account

---

## 📋 5 Quick Start Steps

### Step 1: Verify Environment (3 minutes)

```powershell
# Open PowerShell and enter project directory
cd C:\Users\yli\Desktop\Prefect_Project

# Check Python version
python --version
# Expected: Python 3.11.9+

# Check virtual environment
python -c "import sys; print(sys.executable)"
# Expected: Path containing 'venv' or virtual environment name
```

✅ **Verification Checklist**:
- [ ] Python version ≥ 3.11
- [ ] Running in virtual environment

---

### Step 2: Install Dependencies (2 minutes)

```powershell
# Install project dependencies
pip install -r requirements.txt

# Verify installation
python -c "import prefect, pandas, requests; print('✅ Dependencies installed successfully')"
```

✅ **Verification Checklist**:
- [ ] prefect installed
- [ ] pandas installed
- [ ] requests installed

---

### Step 3: Login to Cloud (2 minutes)

```powershell
# Login to Prefect Cloud
prefect cloud login

# System will prompt for API Key
# Get API Key from https://app.prefect.cloud
```

✅ **Verification Checklist**:
- [ ] Have Prefect Cloud account
- [ ] Logged in to Cloud

---

### Step 4: Deploy (2 minutes)

```powershell
# Deploy all Flows to Cloud
prefect deploy

# Expected output:
# Deployment 'currency-acquisition/Currency Acquisition' created
# Deployment 'prepare-batch/Prepare Batch' created
# Deployment 'process-batch/Process Batch' created
```

✅ **Verification Checklist**:
- [ ] All 3 Deployments created
- [ ] Visible in Cloud UI

---

### Step 5: Configure Schedule (15 minutes)

#### Method A: UI Configuration (Recommended)

Visit https://app.prefect.cloud

**Create 1st Schedule (currency-acquisition)**:
1. Go to **Deployments** → **currency-acquisition**
2. Click **Schedules** tab
3. Click **Create Schedule**
4. Fill in:
   - Cron: `0 9 15,25,28,29,30,31 * *`
   - Timezone: `Asia/Shanghai`
5. Check **Enabled**
6. Click **Save**

**Create 2nd Schedule (prepare-batch)**:
Repeat above steps, use:
- Cron: `30 9 15,25,28,29,30,31 * *`

**Create 3rd Schedule (process-batch)**:
Repeat above steps, use:
- Cron: `0 10 15,25,28,29,30,31 * *`

#### Method B: Quick Reference Table

| Flow | Cron | Time |
|------|------|------|
| currency-acquisition | `0 9 15,25,28,29,30,31 * *` | 09:00 |
| prepare-batch | `30 9 15,25,28,29,30,31 * *` | 09:30 |
| process-batch | `0 10 15,25,28,29,30,31 * *` | 10:00 |

✅ **Verification Checklist**:
- [ ] Created 3 Schedules
- [ ] All Schedules enabled
- [ ] Timezone set to Asia/Shanghai

---

## 🔄 Additional: Start Worker

> ⚠️ **Important**: Worker must run for Cloud to trigger Flows remotely

```powershell
# Run in new PowerShell window (keep it open)
prefect worker start --pool Yichen_Test

# Expected output:
# Worker 'Yichen_Test' started polling for work
# Watching for flow runs from pool 'Yichen_Test'...
```

> 💡 **Tip**: Don't close this window! Worker needs to keep running.

---

## ✅ Verify Deployment Success

### Check 1: Cloud UI

Visit https://app.prefect.cloud

**Deployments Page**:
```
✅ currency-acquisition
   Status: Ready
   Last run: N/A (not run yet)
   Next run: 2025-01-15 09:00

✅ prepare-batch
   Status: Ready
   Next run: 2025-01-15 09:30

✅ process-batch
   Status: Ready
   Next run: 2025-01-15 10:00
```

### Check 2: Command Line Verification

```powershell
# View deployment list
prefect deployment ls
# Should show 3 Deployments

# View Schedule list
prefect deployment schedule ls
# Should show 3 Schedules
```

### Check 3: Manual Test (Optional)

```powershell
# Manually trigger first Flow
prefect deployment run currency-acquisition

# Expected output:
# Submitted flow run 'abc123...'
# Check it out on Cloud UI: https://app.prefect.cloud/...
```

Check Cloud UI logs:
```
✅ Flow started
✅ Fetching exchange rates...
✅ Retrieved 118 countries
✅ Flow completed successfully
```

---

## 🎯 You're Done!

When you see all these ✅, deployment is successful:

- [x] 3 Deployments created
- [x] 3 Schedules configured
- [x] Worker started
- [x] Cloud UI shows next run time
- [x] Manual test passed (optional)

**Expected Auto-Execution Schedule**:

| Date | Time | Action |
|------|------|--------|
| 15th | 09:00 | Fetch rates |
|  | 09:30 | Prepare data |
|  | 10:00 | Process data |
| 25th | 09:00 | Fetch rates |
|  | 09:30 | Prepare data |
|  | 10:00 | Process data |
| 28-31st | Same | Same |

---

## ❓ FAQ

### Q: Flow not triggering at scheduled time?

**Checklist**:
1. Is Worker running?
   ```powershell
   prefect worker inspect Yichen_Test
   ```

2. Is Schedule enabled?
   - Go to Cloud UI → Deployments → [Flow] → Schedules
   - Check if status is "Enabled"

3. Is current time past next trigger time?
   - Check "Next run" time in Cloud UI
   - Must wait until that time to trigger

### Q: "No worker is available"?

**Solution**: Start Worker
```powershell
prefect worker start --pool Yichen_Test
```
Ensure window stays open showing:
```
Worker 'Yichen_Test' started polling for work
```

### Q: How to manually run Flow?

```powershell
# Method 1: Command line
prefect deployment run currency-acquisition

# Method 2: Cloud UI
# Deployments → [Flow] → Custom runs
# Click "Run"
```

### Q: Where are logs?

**Local logs**:
```
6_logs/ directory
```

**Cloud logs**:
```
Cloud UI → Deployments → [Flow] → Latest runs → [Run] → Logs
```

### Q: How long until data output?

**Process Timeline**:
- Exchange rate fetch: ~45 seconds → CSV file
- Data preparation: ~10 seconds → Manifest file
- Data processing: ~10 seconds → Archive complete
- **Total**: ~65 seconds

### Q: Where is the data?

```
By workflow stage:

currency-acquisition:
  Output → data/exchange_rates.csv

prepare-batch:
  Output → 2_preprocessing/manifest_*.json

process-batch:
  Output → 4_archive/* (final archive)
        → 6_logs/* (execution logs)
        → 5_error/* (error records, if any)
```

---

## 📚 Need More Help?

| Need | Reference Document |
|------|-------------------|
| Detailed setup | [SCHEDULE_SETUP_GUIDE_EN.md](SCHEDULE_SETUP_GUIDE_EN.md) |
| Cloud UI details | [SCHEDULE_SETUP_GUIDE_EN.md](SCHEDULE_SETUP_GUIDE_EN.md) |
| Full checklist | [PRODUCTION_DEPLOYMENT_CHECKLIST_EN.md](PRODUCTION_DEPLOYMENT_CHECKLIST_EN.md) |
| API details | [EXCHANGE_RATE_FETCHER_NOTES_EN.md](EXCHANGE_RATE_FETCHER_NOTES_EN.md) |
| Project summary | [PROJECT_COMPLETION_SUMMARY_EN.md](PROJECT_COMPLETION_SUMMARY_EN.md) |
| Troubleshooting | [PRODUCTION_DEPLOYMENT_CHECKLIST_EN.md](PRODUCTION_DEPLOYMENT_CHECKLIST_EN.md) |

---

## 🔧 Quick Command Reference

```powershell
# ===== Deployment =====
prefect deploy                          # Deploy Flows
prefect deployment ls                   # List deployments
prefect deployment run [NAME]           # Manual trigger

# ===== Worker =====
prefect worker start --pool Yichen_Test # Start Worker
prefect worker inspect Yichen_Test      # Check status

# ===== Schedule =====
prefect deployment schedule ls          # List Schedules

# ===== Logs =====
prefect flow-run ls                     # List all runs
prefect flow-run logs [RUN_ID]          # View run logs

# ===== Cloud =====
prefect cloud login                     # Login to Cloud
prefect cloud workspace ls              # List workspaces
```

---

## ⚡ Quick Cheat Sheet

**3 Most Important Cron Expressions**:

```
Fetch rates        09:00   0 9 15,25,28,29,30,31 * *
Prepare batch      09:30   30 9 15,25,28,29,30,31 * *
Process batch      10:00   0 10 15,25,28,29,30,31 * *
```

**3 Most Important Commands**:

```powershell
prefect deploy                          # Deploy
prefect worker start --pool Yichen_Test # Start Worker
prefect deployment run [NAME]           # Manual run
```

**3 Most Important Links**:

```
Cloud UI: https://app.prefect.cloud
Cron Help: https://crontab.guru
Home: README_EN.md
```

---

## ✨ Now You Can:

✅ Auto-fetch monthly exchange rates
✅ Auto-process and archive data by schedule
✅ Monitor execution status in Cloud UI
✅ View detailed execution logs
✅ Manually trigger for testing

---

**Deployment Date**: 2025-01  
**Expected First Run**: 2025-01-15 09:00  
**Timezone**: Asia/Shanghai

Happy deploying! 🎉
