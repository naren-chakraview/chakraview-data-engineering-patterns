from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

all_assets = load_assets_from_modules([assets])

daily_pipeline = define_asset_job(
    name="daily_pipeline",
    selection="*",
)

daily_schedule = ScheduleDefinition(
    job=daily_pipeline,
    cron_schedule="@daily",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    jobs=[daily_pipeline],
    schedules=[daily_schedule],
)
