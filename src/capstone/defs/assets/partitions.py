# src/dagster_essentials/defs/partitions.py
import dagster as dg


start_date = '2025-10-01'
end_date = '2025-10-20'

monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)


daily_partition = dg.DailyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
