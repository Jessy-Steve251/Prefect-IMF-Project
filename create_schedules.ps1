# 创建 Prefect Deployment Schedule
# 这个脚本为三个 Deployment 创建时间表

Write-Host "======================================================"
Write-Host "Creating Prefect Deployment Schedules"
Write-Host "======================================================"
Write-Host ""

# 第一个 Schedule: currency-acquisition (09:00)
Write-Host "Creating schedule for currency-acquisition..."
python -m prefect deployment schedule create `
  "currency_acquisition_flow/currency-acquisition" `
  --cron "0 9 15,25,28,29,30,31 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ currency-acquisition schedule created"
Write-Host ""

# 第二个 Schedule: prepare-batch (09:30)
Write-Host "Creating schedule for prepare-batch..."
python -m prefect deployment schedule create `
  "prepare_batch_flow/prepare-batch" `
  --cron "30 9 15,25,28,29,30,31 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ prepare-batch schedule created"
Write-Host ""

# 第三个 Schedule: process-batch (10:00)
Write-Host "Creating schedule for process-batch..."
python -m prefect deployment schedule create `
  "process_batch_flow/process-batch" `
  --cron "0 10 15,25,28,29,30,31 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ process-batch schedule created"
Write-Host ""

Write-Host "======================================================"
Write-Host "✅ All Schedules Created Successfully!"
Write-Host "======================================================"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Go to https://app.prefect.cloud"
Write-Host "2. Navigate to Deployments"
Write-Host "3. Click on each deployment"
Write-Host "4. Verify schedules appear in the Schedules tab"
Write-Host ""
