from celery.schedules import crontab

from web.admin_misc_routes import _format_schedule_interval


def test_format_schedule_interval_formats_second_intervals():
    assert _format_schedule_interval(120) == "2m"
    assert _format_schedule_interval(7200) == "2h"
    assert _format_schedule_interval(45) == "45s"


def test_format_schedule_interval_formats_daily_crontab():
    assert _format_schedule_interval(crontab(hour=6, minute=0)) == "daily 06:00"


def test_format_schedule_interval_formats_generic_crontab():
    assert _format_schedule_interval(crontab(minute="*/5", hour="*")) == "cron */5 * * * *"
