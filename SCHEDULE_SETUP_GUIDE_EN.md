# Prefect Cloud Schedule Configuration Guide (English Version)

## Quick Summary

You need to manually configure three schedules in Prefect Cloud UI:

| Flow | Execution Time | Cron Expression |
|------|----------------|-----------------|
| currency-acquisition | Monthly 09:00 | `0 9 15,25,28,29,30,31 * *` |
| prepare-batch | Monthly 09:30 | `30 9 15,25,28,29,30,31 * *` |
| process-batch | Monthly 10:00 | `0 10 15,25,28,29,30,31 * *` |

---

## 📋 Configuration Steps

### Step 1: Login to Prefect Cloud

1. Open browser and visit: https://app.prefect.cloud
2. Login with your account

### Step 2: Setup Schedule for currency-acquisition

1. In left menu select **"Deployments"** or **"Flows"**
2. Find **"currency_acquisition_flow"** flow
3. Click the flow to enter details page
4. Find **"Schedules"** or **"Add Schedule"** button
5. Click **"Create Schedule"**
6. Configure as follows:
   - **Cron Expression**: `0 9 15,25,28,29,30,31 * *`
   - **Timezone**: Select your timezone (e.g., Europe/Zurich)
   - **Description**: "Fetch exchange rate data on 15th, 25th, 28-31st every month"
   - **Active**: Check to enable
7. Click **"Save"** or **"Create"**

### Step 3: Setup Schedule for prepare-batch

Repeat Step 2 with the following configuration:
- **Flow**: prepare_batch_flow
- **Cron Expression**: `30 9 15,25,28,29,30,31 * *`
- **Description**: "Prepare batch data on 15th, 25th, 28-31st every month"

### Step 4: Setup Schedule for process-batch

Repeat Step 2 with the following configuration:
- **Flow**: process_batch_flow
- **Cron Expression**: `0 10 15,25,28,29,30,31 * *`
- **Description**: "Process batch data on 15th, 25th, 28-31st every month"

---

## 🔍 Cron Expression Explanation

```
0 9 15,25,28,29,30,31 * *
│ │ │                  │ │
│ │ │                  │ └─── Day of week (0-6, where 0=Sunday, * means every day)
│ │ │                  └────── Month (1-12, * means every month)
│ │ └──────────────────────── Day of month (1-31)
│ │                           Here: 15, 25, 28, 29, 30, 31
│ └──────────────────────────── Hour (0-23)
│                              Here: 9 = 09:00
└─────────────────────────────── Minute (0-59)
                                Here: 0 = :00
```

### Cron Expression Examples for Different Needs

| Requirement | Cron Expression |
|-------------|-----------------|
| Every 15th at 09:00 | `0 9 15 * *` |
| 15th and 25th at 09:00 | `0 9 15,25 * *` |
| 15th through 31st at 09:00 | `0 9 15-31 * *` |
| 15th, 25th, 28-31st at 09:00 | `0 9 15,25,28-31 * *` |
| 28th through 31st at 09:00 | `0 9 28-31 * *` |

---

## ⏰ Execution Schedule

Recommended execution sequence:

```
09:00 - currency-acquisition runs (Fetch exchange rate data)
        ↓
09:30 - prepare-batch runs (Prepare batch data using newly fetched rates)
        ↓
10:00 - process-batch runs (Process batch data)
        ↓
10:30 - All tasks complete
```

This ensures proper data flow order.

---

## 🧪 Testing Schedules

### Method 1: Wait for scheduled time (Not recommended)
- Wait for next trigger time and manually check

### Method 2: Manual trigger in Cloud UI (Recommended)
1. Find the corresponding deployment in Prefect Cloud
2. Click **"Run"** or **"Trigger"** button
3. Check run logs and results

### Method 3: Test locally, then deploy
```bash
# Run flows locally to verify logic
python -m flows.currency_acquisition_flow
python -m flows.prepare_batch_flow
python -m flows.process_batch_flow

# Then setup schedules in Cloud
```

---

## 📌 Important Notes

### 1. Timezone Handling
- Cron expressions are relative to the selected timezone
- Ensure you select the correct timezone (e.g., Europe/Zurich, Asia/Shanghai)
- Different months have different number of days (automatically handled)

### 2. End-of-Month Date Handling
- Some months don't have 29, 30, or 31 days
- Cron automatically skips non-existent dates
- Example: February skips 29, 30, 31 in non-leap years

### 3. Worker/Agent Relationship
- Cron scheduling happens on Prefect Cloud side
- Cloud creates flow run at scheduled time
- Run executes when a worker is available
- Ensure at least one worker is running

### 4. Timezone Reference
Prefect supports common timezones:
- `UTC` - Coordinated Universal Time
- `Asia/Shanghai` - China Standard Time
- `America/New_York` - Eastern Time
- `Europe/London` - Greenwich Mean Time
- [Full timezone list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)

---

## 📊 Check Schedule Status

1. **View in Cloud UI**
   - Flows → Select flow → Schedules tab
   - See all schedules and their status

2. **View past runs**
   - Flows → Select flow → Runs tab
   - See run history and logs

3. **Check upcoming runs**
   - After schedule creation, Cloud shows next run time

---

## 🚀 Complete Workflow After Setup

```
On 15th, 25th, 28-31st of every month:

09:00:00 - Cloud triggers currency-acquisition
           ↓
           Fetch exchange rate data from IMF API
           ↓
           Save exchange_rates_2025_XX.csv
           ✅ Complete (approximately 30-60 seconds)

09:30:00 - Cloud triggers prepare-batch
           ↓
           Read newly generated exchange rate data
           ↓
           Prepare batch files
           ↓
           Generate MANIFEST.json
           ✅ Complete (approximately 10 seconds)

10:00:00 - Cloud triggers process-batch
           ↓
           Read MANIFEST.json
           ↓
           Process data and archive
           ✅ Complete (approximately 10 seconds)

10:05:00 - All tasks complete ✨
```

---

## ❓ FAQ

### Q1: Schedule not executing?
- Verify Prefect Cloud schedule is enabled
- Confirm a worker is running
- Verify timezone setting is correct

### Q2: How to modify existing schedule?
- Find the schedule in Cloud UI
- Click edit button
- Modify cron expression or timezone
- Save changes

### Q3: How to disable schedule?
- Find the schedule in Cloud UI
- Click disable button or delete

### Q4: Can I set multiple different schedules?
- Yes! Each deployment can have multiple schedules
- Example: Both monthly and manual trigger possible

---

## 📞 Need Help?

Reference official Prefect documentation:
- [Schedules and Deployments](https://docs.prefect.io/latest/concepts/deployments/)
- [Cron Syntax](https://en.wikipedia.org/wiki/Cron#Overview)
- [Prefect Cloud UI Guide](https://docs.prefect.io/latest/cloud/cloud-ui/)
