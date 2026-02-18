# 更新 Prefect Deployment Schedules
# 将 15 号改成 11 号，时间改成 11:00

Write-Host "======================================================"
Write-Host "Updating Prefect Deployment Schedules"
Write-Host "======================================================"
Write-Host ""

# 获取现有的 Schedule 并删除
Write-Host "Clearing old schedules..."
python -m prefect deployment schedule clear "currency_acquisition_flow/currency-acquisition" -y
python -m prefect deployment schedule clear "prepare_batch_flow/prepare-batch" -y
python -m prefect deployment schedule clear "process_batch_flow/process-batch" -y

Write-Host ""
Write-Host "Creating new schedules with updated times..."
Write-Host ""

# 第一个 Schedule: currency-acquisition (11:00 on 11th)
Write-Host "Creating schedule for currency-acquisition (11:00 on 11th)..."
python -m prefect deployment schedule create `
  "currency_acquisition_flow/currency-acquisition" `
  --cron "0 11 11 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ currency-acquisition schedule created"
Write-Host ""

# 第二个 Schedule: prepare-batch (11:30 on 11th)
Write-Host "Creating schedule for prepare-batch (11:30 on 11th)..."
python -m prefect deployment schedule create `
  "prepare_batch_flow/prepare-batch" `
  --cron "30 11 11 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ prepare-batch schedule created"
Write-Host ""

# 第三个 Schedule: process-batch (12:00 on 11th)
Write-Host "Creating schedule for process-batch (12:00 on 11th)..."
python -m prefect deployment schedule create `
  "process_batch_flow/process-batch" `
  --cron "0 12 11 * *" `
  --timezone "Asia/Shanghai" `
  -y

Write-Host "✅ process-batch schedule created"
Write-Host ""

Write-Host "======================================================"
Write-Host "✅ All Schedules Updated Successfully!"
Write-Host "======================================================"
Write-Host ""
Write-Host "New schedule:"
Write-Host "  - 11:00 on 11th: currency-acquisition"
Write-Host "  - 11:30 on 11th: prepare-batch"
Write-Host "  - 12:00 on 11th: process-batch"
Write-Host ""
Write-Host "Next steps:"
Write-Host "1. Start the Worker:"
Write-Host "   prefect worker start --pool Yichen_Test"
Write-Host "2. Tomorrow (Dec 11) at 11:00, flows will execute automatically"
Write-Host "3. Monitor in https://app.prefect.cloud"
Write-Host ""
