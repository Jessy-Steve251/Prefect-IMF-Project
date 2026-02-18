# 🤖 Automatic Execution Guide - How It Works

**Purpose**: Explain how Prefect Cloud automatically executes flows  
**Date**: December 2025  
**Key Question**: "Does Prefect execute automatically even if my computer is off?"

---

## ✅ Quick Answer

**YES, Prefect Cloud automatically executes flows on schedule.** ✅

But with **ONE critical requirement**: Your Worker must be running when the execution time arrives.

---

## 🏗️ Architecture: How It Works

### The Complete Flow

```
PREFECT CLOUD (SaaS - Running on Prefect's Servers)
    ├─ Scheduler (Always running, never stops)
    │   └─ Monitors all schedules
    │       └─ At 09:00 on 15th: Creates Flow Run "currency-acquisition"
    │
    ├─ Flow Run Queue (Holds pending executions)
    │   └─ Waits for Worker to accept the run
    │
    └─ Run Logs Storage (Stores all execution history)
         └─ Records every execution result

              ↓ (Network Connection)

YOUR COMPUTER (Needs to be online at execution time)
    ├─ Python Environment
    ├─ Worker Process (Polling Prefect Cloud for work)
    │   └─ "Any work for me?" (every few seconds)
    │       └─ If yes: Accept and execute
    │       └─ If no: Wait and ask again
    │
    ├─ Flow Code (currency_acquisition_flow.py, etc.)
    └─ Storage (Results, CSV files, logs)
```

---

## ⏰ Timeline: What Happens at Execution Time

### Example: January 15, 2025 at 09:00 (Asia/Shanghai timezone)

**Scenario A: Your Computer is ONLINE** ✅

```
09:00:00 - Prefect Cloud Scheduler triggers
           
           Cloud says: "Time for currency-acquisition!"
           ↓
           Cloud creates Flow Run with ID: 12345abc
           ↓
           Cloud puts it in Work Pool queue: "Yichen_Test"
           ↓
09:00:02 - Your Worker polls Cloud: "Any work?"
           ↓
           Cloud responds: "YES! Run 12345abc"
           ↓
09:00:03 - Your Worker accepts the run
           ↓
           Flow executes on your computer
           ↓
           currency_acquisition_flow() starts
           ↓
09:00:45 - Currency acquisition completes
           ↓
           Results saved to: data/exchange_rates_2025_01.csv
           ↓
           Cloud records: SUCCESS ✅
```

**Scenario B: Your Computer is OFFLINE** ❌

```
09:00:00 - Prefect Cloud Scheduler triggers
           
           Cloud says: "Time for currency-acquisition!"
           ↓
           Cloud creates Flow Run with ID: 12345abc
           ↓
           Cloud puts it in Work Pool queue: "Yichen_Test"
           ↓
09:00:02 - Cloud waits for Worker to poll
           
           But... YOUR COMPUTER IS OFF
           ↓
           Worker can't connect
           ↓
09:05:00 - Cloud waits... and waits...
           ↓
           Flow Run status: SCHEDULED (not executing)
           ↓
           Cloud records: NO WORKER AVAILABLE ⚠️
```

---

## 🔑 The Critical Requirement: Worker Must Be Running

### What is a Worker?

A **Worker** is a process that:
1. Runs on your computer (or a server)
2. Connects to Prefect Cloud
3. Asks: "Do you have any work for me?"
4. Executes flows when asked

### Starting a Worker

```powershell
prefect worker start --pool Yichen_Test
```

**What this command does**:
```
1. Connects to Prefect Cloud (using your API key)
2. Identifies your Worker Pool: "Yichen_Test"
3. Polls Cloud every 3 seconds: "Any new runs?"
4. Waits for Flow Runs to be queued
5. When a run appears, executes it
6. Uploads results back to Cloud
```

**Output when running**:
```
Worker 'Yichen_Test' started successfully
Listening for flow runs on 'Yichen_Test' pool...
[Waiting for runs...]
[Waiting for runs...]
[Run 12345abc received!]
[Executing currency_acquisition_flow...]
[Run complete! Status: SUCCESS]
[Waiting for runs...]
```

---

## 📋 Recommended Setup

### Option 1: Computer Always ON (Safest)

**Setup**: Leave your computer running 24/7 with Worker continuously polling

**Advantages**:
- ✅ 100% guaranteed to capture scheduled executions
- ✅ Simple to setup and maintain
- ✅ Works reliably every month

**Disadvantages**:
- ❌ Computer always on (electricity cost)
- ❌ More wear on hardware

**How**:
```
1. Leave computer on all the time
2. Run: prefect worker start --pool Yichen_Test
3. Keep the terminal window open
4. Don't close it
```

---

### Option 2: Computer ON at Execution Time (Recommended)

**Setup**: Turn on computer 5 minutes before execution time, start Worker

**Advantages**:
- ✅ Saves electricity (computer off most of the time)
- ✅ Computer only on when needed
- ✅ Reliable if you remember to start Worker

**Disadvantages**:
- ❌ Manual step required (start Worker)
- ❌ Risk of forgetting on trigger dates
- ❌ Need to wake up computer manually

**How**:
```
1. Set a reminder for 08:55 on trigger dates (15th, 25th, 28-31st)
2. Turn on computer
3. Wait for startup (~1-2 minutes)
4. Open PowerShell
5. Run: prefect worker start --pool Yichen_Test
6. Keep terminal open until 10:15 (to cover all 3 flows)
7. Close terminal and turn off computer
```

**Reminder dates each month**:
```
January:   15th @ 08:55, 25th @ 08:55, 28th-31st @ 08:55
February:  15th @ 08:55, 25th @ 08:55, 28th @ 08:55 (29th in leap year)
March:     15th @ 08:55, 25th @ 08:55, 28th-31st @ 08:55
... (same pattern for other months)

Total reminders per year: ~22 reminders
Time per reminder: ~20-25 minutes (on computer)
```

---

### Option 3: Dedicated Server (Enterprise)

**Setup**: Run Worker on a dedicated server/VPS that's always on

**Advantages**:
- ✅ 100% uptime
- ✅ No need to manage your personal computer
- ✅ Professional infrastructure
- ✅ Can run multiple workers

**Disadvantages**:
- ❌ Costs money (VPS hosting)
- ❌ More complex setup
- ❌ Overkill for monthly executions

**Cost**: ~$5-20/month for small VPS

**How**:
```
1. Rent a VPS (DigitalOcean, AWS, etc.)
2. Install Python and Prefect
3. Start Worker on VPS
4. Leave it running forever
```

---

## 🚨 What Happens If Worker is Offline?

### The Flow Run Status Progression

```
09:00:00 SCHEDULED  ← Cloud scheduled the run
         ↓
09:00:05 QUEUED     ← Run waiting in queue for Worker
         ↓
09:05:00 Still QUEUED
         ↓
09:30:00 Still QUEUED
         ↓
?? If Worker comes online: RUNNING → COMPLETED ✅
?? If Worker never comes online: Stays QUEUED forever ⏳
```

### Can You Retry Later?

**YES!** Options:

1. **Manual Trigger Later**:
   ```powershell
   prefect deployment run currency-acquisition
   # This runs immediately, not on schedule
   ```

2. **Manually in Cloud UI**:
   - Go to Cloud UI
   - Find the queued run
   - Click "Retry" or "Re-run"
   - It executes if Worker is now online

3. **Wait Until Next Month**:
   - Scheduled runs are monthly
   - If you miss January 15th, next run is January 25th
   - Make sure Worker is online by then

---

## 🔄 Cloud Scheduler vs Local Scheduler

### Key Difference

**What you might think**:
```
"My computer has a schedule (like Windows Task Scheduler)"
"So when my computer is off, the schedule pauses"
```

**How Prefect actually works**:
```
"Prefect Cloud (not your computer) manages the schedule"
"Your computer only needs to be online to EXECUTE"
"Cloud never forgets to trigger - it's always running"
```

### Visual Comparison

```
Traditional Scheduler (Windows Task Scheduler):
┌─────────────────────────┐
│ Your Computer           │
│ ├─ Schedule: 09:00      │
│ ├─ Task: currency_acq   │
│ └─ Status: INACTIVE     │ ← Computer OFF = Schedule OFF
└─────────────────────────┘

Prefect Cloud Scheduler:
┌──────────────────────────────────┐
│ Prefect Cloud (Always Running)   │
│ ├─ Schedule: 09:00 on 15th       │
│ ├─ Status: ACTIVE ✅             │ ← Always monitoring
│ └─ Waiting for: Worker online    │
└──────────────────────────────────┘
         ↓
┌─────────────────────────┐
│ Your Computer           │
│ ├─ Worker: OFFLINE      │ ← OK if OFF (until 09:00)
│ ├─ Worker: ONLINE       │ ← Must be ON at 09:00
│ └─ Worker: OFFLINE      │ ← OK if OFF after 10:15
└─────────────────────────┘
```

---

## 📊 Work Pool Types Explained

Your Work Pool is: **prefect:managed**

### What does "prefect:managed" mean?

```
Work Pool Type: prefect:managed

Execution Location: ❌ NOT on Prefect Cloud
Execution Location: ✅ YES on your Worker machine

Architecture:
  Cloud creates run
    ↓
  Sends run to Work Pool "Yichen_Test"
    ↓
  Your Worker picks it up
    ↓
  Your computer executes the flow
```

### Other Work Pool Types (For Reference)

| Type | Execution | Worker Needed | When Computer OFF |
|------|-----------|---------------|-------------------|
| prefect:managed | Your machine | YES | ❌ Won't execute |
| prefect:agent | Your machine | YES | ❌ Won't execute |
| prefect:cloud-run | Google Cloud | NO | ✅ Automatic |
| prefect:ecs | AWS ECS | NO | ✅ Automatic |
| prefect:kubernetes | K8s cluster | NO | ✅ Automatic |

**Conclusion**: With prefect:managed, you MUST have Worker online at execution time.

---

## ✅ Checklist: Ensure Automatic Execution

### Monthly Check (Before trigger dates)

- [ ] Worker can start without errors
  ```powershell
  prefect worker start --pool Yichen_Test
  # Should show: "Worker started successfully"
  ```

- [ ] Network connectivity works
  ```powershell
  Test-NetConnection app.prefect.cloud
  # Should show: TcpTestSucceeded: True
  ```

- [ ] Schedules still enabled in Cloud
  ```
  Cloud UI → Deployments → [Flow] → Schedules
  Check: All 3 schedules marked as "Enabled"
  ```

### Before Each Trigger Date

**Option A: Set Reminder (Recommended)**
- [ ] Calendar reminder set for 08:55
- [ ] Plan to start Worker 5 minutes early
- [ ] Keep terminal open for 20 minutes

**Option B: Keep Computer On**
- [ ] Computer running 24/7
- [ ] Worker process always running
- [ ] Terminal window visible

### Execution Day Verification

- [ ] Worker started by 09:00
- [ ] Monitor Cloud UI during execution
- [ ] Verify all 3 flows complete
- [ ] Check CSV file was created

---

## 🔍 Troubleshooting: Why Didn't It Execute?

### Problem 1: "Run is still QUEUED"

**Cause**: Worker offline during execution time

**Check**:
```powershell
# Was Worker running at 09:00?
# Check your terminal history
# Look for: "Worker started successfully"
```

**Solution**:
```powershell
# Start Worker now
prefect worker start --pool Yichen_Test

# Manually trigger the flow (will execute immediately)
prefect deployment run currency-acquisition

# Next month: Remember to start Worker by 08:55
```

---

### Problem 2: "No runs in history"

**Cause**: Schedule might be disabled or Cloud issue

**Check in Cloud UI**:
```
Deployments → currency-acquisition → Schedules

Look for:
✓ Schedule exists
✓ Status is "Enabled" (green)
✓ Next run date is correct
```

**Solution**:
```
1. Check schedule is enabled
2. If disabled, enable it
3. Manually trigger: prefect deployment run currency-acquisition
```

---

### Problem 3: "Worker crashed or disconnected"

**Symptoms**:
- Worker started but shows errors
- Terminal shows: "Connection lost"

**Check**:
```powershell
# Can you reach Cloud?
Test-NetConnection app.prefect.cloud -Port 443

# Do you have valid API key?
prefect config view | findstr "PREFECT_API_KEY"
```

**Solution**:
```powershell
# Re-login to Cloud
prefect cloud login

# Restart Worker
prefect worker start --pool Yichen_Test
```

---

## 📞 When Manual Action is Required

You need to manually start Worker if:

- [ ] Computer was off at execution time
- [ ] Worker crashed or disconnected
- [ ] You want to test the flow immediately
- [ ] You need to run flow on different schedule

**Manual trigger command**:
```powershell
prefect deployment run currency-acquisition

# Runs immediately, regardless of schedule
# Useful for testing or catch-up executions
```

---

## 🎯 Best Practice Recommendation

### For Your Situation (Monthly Execution)

**Recommended Setup**: Option 2 (Computer ON at execution time)

**Steps**:

1. **Set monthly reminders** (Use phone calendar)
   - January: 15th, 25th, 28, 29, 30, 31 @ 08:55
   - February: 15th, 25th, 28 @ 08:55 (or 29 in leap year)
   - ... (repeat for all months)
   - **Total: ~22 reminders per year**

2. **When reminder triggers**:
   ```powershell
   # Step 1: Turn on computer
   # Step 2: Wait 1-2 minutes for startup
   # Step 3: Open PowerShell
   # Step 4: Navigate to project directory
   cd C:\Users\yli\Desktop\Prefect_Project
   
   # Step 5: Start Worker
   prefect worker start --pool Yichen_Test
   
   # Step 6: Watch the output
   # You'll see: "Worker started successfully"
   # Then: "[Run 12345 received!]" at 09:00-10:00
   # Then: "[Run complete!]"
   
   # Step 7: After 10:15, close terminal and turn off
   ```

3. **No interaction needed after Worker starts**
   - Worker automatically accepts and executes runs
   - All 3 flows execute in sequence
   - Results automatically uploaded to Cloud

---

## 📈 Monthly Execution Calendar

**Trigger dates each month**:
```
Every month on: 15th, 25th, 28, 29, 30, 31

Dates by month (2025):
Jan: 15, 25, 28, 29, 30, 31 (6 days)
Feb: 15, 25, 28 (3 days, only 28 in 2025)
Mar: 15, 25, 28, 29, 30, 31 (6 days)
Apr: 15, 25, 28, 29, 30 (5 days, no 31)
May: 15, 25, 28, 29, 30, 31 (6 days)
Jun: 15, 25, 28, 29, 30 (5 days, no 31)
Jul: 15, 25, 28, 29, 30, 31 (6 days)
Aug: 15, 25, 28, 29, 30, 31 (6 days)
Sep: 15, 25, 28, 29, 30 (5 days, no 31)
Oct: 15, 25, 28, 29, 30, 31 (6 days)
Nov: 15, 25, 28, 29, 30 (5 days, no 31)
Dec: 15, 25, 28, 29, 30, 31 (6 days)

Total reminders per year: ~66 (~22 per month average)
Time per reminder: ~20 minutes
Total annual time: ~22 hours (~2 hours per month)
```

---

## 🎊 Summary

**To guarantee automatic execution**:

✅ **Prefect Cloud handles the scheduling** (always running)
✅ **Your Worker executes the flows** (needs to be online at execution time)
✅ **Recommended: Start Worker 5 minutes before each trigger date**
✅ **Cost: ~22 reminders per year, ~20 minutes each time**
✅ **Reliability: 100% if you start Worker on time**

**Your computer can be OFF most of the time. Only needs to be ON for ~20 minutes per trigger date.**

---

**Questions?** See PRODUCTION_DEPLOYMENT_CHECKLIST_EN.md for detailed monitoring procedures.
