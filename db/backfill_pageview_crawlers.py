from __future__ import annotations

from dataclasses import dataclass
import sys
from pathlib import Path

from sqlalchemy import bindparam, text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.models import engine

PROBE_PATH_REGEX = (
    r"^/\.git/|^/\.env|^/wp-|^/wordpress|^/xmlrpc\.php$|^/phpmyadmin|"
    r"^/boaform|^/cgi-bin|^/actuator|^/vendor/|^/server-status|"
    r"^/login\.php$|\.php$|\.asp$|\.aspx$"
)


@dataclass(frozen=True)
class BackfillResult:
    probe_rows_marked: int
    auth_spray_rows_marked: int
    auth_spray_ip_count: int


def backfill_pageview_crawlers() -> BackfillResult:
    probe_rows_marked = 0
    auth_spray_rows_marked = 0
    auth_spray_ip_count = 0

    with engine.begin() as conn:
        probe_update = conn.execute(
            text(
                """
                UPDATE PageView
                SET is_crawler = 1,
                    crawler_name = 'probe-bot'
                WHERE (is_crawler = 0 OR is_crawler IS NULL)
                  AND path REGEXP :probe_regex
                """
            ),
            {"probe_regex": PROBE_PATH_REGEX},
        )
        probe_rows_marked = int(probe_update.rowcount or 0)

        suspicious_ips = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT ip_address
                    FROM PageView
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND path = '/auth/login'
                      AND (referrer IS NULL OR referrer = '')
                      AND user_agent IN (
                          SELECT user_agent
                          FROM PageView
                          WHERE (is_crawler = 0 OR is_crawler IS NULL)
                            AND path = '/auth/login'
                            AND (referrer IS NULL OR referrer = '')
                            AND user_agent IS NOT NULL
                            AND user_agent != ''
                          GROUP BY user_agent
                          HAVING COUNT(*) >= 50
                             AND COUNT(DISTINCT ip_address) >= 20
                      )
                    """
                )
            ).all()
            if row[0]
        ]
        auth_spray_ip_count = len(suspicious_ips)

        if suspicious_ips:
            auth_update = conn.execute(
                text(
                    """
                    UPDATE PageView
                    SET is_crawler = 1,
                        crawler_name = 'auth-spray-bot'
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND ip_address IN :ip_addresses
                    """
                ).bindparams(bindparam("ip_addresses", expanding=True)),
                {"ip_addresses": suspicious_ips},
            )
            auth_spray_rows_marked = int(auth_update.rowcount or 0)

    return BackfillResult(
        probe_rows_marked=probe_rows_marked,
        auth_spray_rows_marked=auth_spray_rows_marked,
        auth_spray_ip_count=auth_spray_ip_count,
    )


if __name__ == "__main__":
    result = backfill_pageview_crawlers()
    print(
        "probe_rows_marked={probe_rows_marked} "
        "auth_spray_rows_marked={auth_spray_rows_marked} "
        "auth_spray_ip_count={auth_spray_ip_count}".format(
            probe_rows_marked=result.probe_rows_marked,
            auth_spray_rows_marked=result.auth_spray_rows_marked,
            auth_spray_ip_count=result.auth_spray_ip_count,
        )
    )
