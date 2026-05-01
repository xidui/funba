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

OBSERVED_CLOUD_SCRAPER_PREFIXES = (
    # Confirmed from current traffic: Hetzner / Alibaba Cloud / scanner ranges
    # sending browser-like UA traffic with high visitor-cookie churn.
    "91.98.",
    "116.202.",
    "49.12.",
    "138.199.",
    "162.55.",
    "167.235.",
    "159.69.",
    "178.63.",
    "157.90.",
    "142.132.",
    "46.4.",
    "168.119.",
    "47.79.",
    "47.82.",
    "205.210.",
    "198.235.",
)


@dataclass(frozen=True)
class BackfillResult:
    probe_rows_marked: int
    auth_spray_rows_marked: int
    auth_spray_ip_count: int
    ip_churn_rows_marked: int
    ip_churn_ip_count: int
    network_churn_rows_marked: int
    network_churn_prefix_count: int
    distributed_proxy_rows_marked: int
    distributed_proxy_prefix_count: int
    observed_cloud_rows_marked: int
    observed_cloud_prefix_count: int


def backfill_pageview_crawlers() -> BackfillResult:
    probe_rows_marked = 0
    auth_spray_rows_marked = 0
    auth_spray_ip_count = 0
    ip_churn_rows_marked = 0
    ip_churn_ip_count = 0
    network_churn_rows_marked = 0
    network_churn_prefix_count = 0
    distributed_proxy_rows_marked = 0
    distributed_proxy_prefix_count = 0
    observed_cloud_rows_marked = 0
    observed_cloud_prefix_count = 0

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

        churn_ips = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT ip_address
                    FROM PageView
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND ip_address IS NOT NULL
                      AND ip_address != ''
                    GROUP BY ip_address
                    HAVING COUNT(*) >= 30
                       AND COUNT(DISTINCT visitor_id) >= 20
                       AND COUNT(DISTINCT user_agent) >= 5
                       AND COUNT(DISTINCT visitor_id) * 100 >= COUNT(*) * 60
                    """
                )
            ).all()
            if row[0]
        ]
        ip_churn_ip_count = len(churn_ips)

        if churn_ips:
            churn_update = conn.execute(
                text(
                    """
                    UPDATE PageView
                    SET is_crawler = 1,
                        crawler_name = 'ip-cookie-churn-scraper'
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND ip_address IN :ip_addresses
                    """
                ).bindparams(bindparam("ip_addresses", expanding=True)),
                {"ip_addresses": churn_ips},
            )
            ip_churn_rows_marked = int(churn_update.rowcount or 0)

        churn_prefixes = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT CONCAT(SUBSTRING_INDEX(ip_address, '.', 2), '.') AS prefix
                    FROM PageView
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND created_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
                      AND ip_address REGEXP '^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$'
                    GROUP BY prefix
                    HAVING COUNT(*) >= 120
                       AND COUNT(DISTINCT visitor_id) >= 80
                       AND COUNT(DISTINCT ip_address) >= 20
                       AND COUNT(DISTINCT user_agent) >= 8
                       AND COUNT(DISTINCT visitor_id) * 100 >= COUNT(*) * 50
                    """
                )
            ).all()
            if row[0]
        ]
        network_churn_prefix_count = len(churn_prefixes)

        for prefix in churn_prefixes:
            prefix_update = conn.execute(
                text(
                    """
                    UPDATE PageView
                    SET is_crawler = 1,
                        crawler_name = 'network-cookie-churn-scraper'
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND ip_address LIKE :prefix_like
                    """
                ),
                {"prefix_like": f"{prefix}%"},
            )
            network_churn_rows_marked += int(prefix_update.rowcount or 0)

        distributed_proxy_prefixes = [
            row[0]
            for row in conn.execute(
                text(
                    """
                    SELECT CONCAT(SUBSTRING_INDEX(ip_address, '.', 2), '.') AS prefix
                    FROM PageView
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND created_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY
                      AND ip_address REGEXP '^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$'
                    GROUP BY prefix
                    HAVING COUNT(*) >= 80
                       AND COUNT(DISTINCT visitor_id) * 100 >= COUNT(*) * 90
                       AND COUNT(DISTINCT ip_address) * 100 >= COUNT(DISTINCT visitor_id) * 85
                       AND COUNT(DISTINCT path) >= 40
                       AND SUM(CASE WHEN referrer IS NULL OR referrer = '' THEN 1 ELSE 0 END) * 100 >= COUNT(*) * 90
                       AND SUM(
                           CASE
                               WHEN referrer IS NOT NULL
                                AND referrer != ''
                                AND referrer NOT LIKE 'http%://funba.app%'
                                AND referrer NOT LIKE 'http%://www.funba.app%'
                               THEN 1 ELSE 0
                           END
                       ) = 0
                       AND SUM(
                           CASE
                               WHEN referrer LIKE 'http%://funba.app%'
                                 OR referrer LIKE 'http%://www.funba.app%'
                               THEN 1 ELSE 0
                           END
                       ) <= 3
                    """
                )
            ).all()
            if row[0]
        ]
        distributed_proxy_prefix_count = len(distributed_proxy_prefixes)

        for prefix in distributed_proxy_prefixes:
            prefix_update = conn.execute(
                text(
                    """
                    UPDATE PageView
                    SET is_crawler = 1,
                        crawler_name = 'distributed-proxy-scraper'
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND created_at >= UTC_TIMESTAMP() - INTERVAL 30 DAY
                      AND ip_address LIKE :prefix_like
                    """
                ),
                {"prefix_like": f"{prefix}%"},
            )
            distributed_proxy_rows_marked += int(prefix_update.rowcount or 0)

        observed_cloud_prefix_count = len(OBSERVED_CLOUD_SCRAPER_PREFIXES)
        for prefix in OBSERVED_CLOUD_SCRAPER_PREFIXES:
            prefix_update = conn.execute(
                text(
                    """
                    UPDATE PageView
                    SET is_crawler = 1,
                        crawler_name = 'observed-cloud-scraper'
                    WHERE (is_crawler = 0 OR is_crawler IS NULL)
                      AND ip_address LIKE :prefix_like
                    """
                ),
                {"prefix_like": f"{prefix}%"},
            )
            observed_cloud_rows_marked += int(prefix_update.rowcount or 0)

    return BackfillResult(
        probe_rows_marked=probe_rows_marked,
        auth_spray_rows_marked=auth_spray_rows_marked,
        auth_spray_ip_count=auth_spray_ip_count,
        ip_churn_rows_marked=ip_churn_rows_marked,
        ip_churn_ip_count=ip_churn_ip_count,
        network_churn_rows_marked=network_churn_rows_marked,
        network_churn_prefix_count=network_churn_prefix_count,
        distributed_proxy_rows_marked=distributed_proxy_rows_marked,
        distributed_proxy_prefix_count=distributed_proxy_prefix_count,
        observed_cloud_rows_marked=observed_cloud_rows_marked,
        observed_cloud_prefix_count=observed_cloud_prefix_count,
    )


if __name__ == "__main__":
    result = backfill_pageview_crawlers()
    print(
        "probe_rows_marked={probe_rows_marked} "
        "auth_spray_rows_marked={auth_spray_rows_marked} "
        "auth_spray_ip_count={auth_spray_ip_count} "
        "ip_churn_rows_marked={ip_churn_rows_marked} "
        "ip_churn_ip_count={ip_churn_ip_count} "
        "network_churn_rows_marked={network_churn_rows_marked} "
        "network_churn_prefix_count={network_churn_prefix_count} "
        "distributed_proxy_rows_marked={distributed_proxy_rows_marked} "
        "distributed_proxy_prefix_count={distributed_proxy_prefix_count} "
        "observed_cloud_rows_marked={observed_cloud_rows_marked} "
        "observed_cloud_prefix_count={observed_cloud_prefix_count}".format(
            probe_rows_marked=result.probe_rows_marked,
            auth_spray_rows_marked=result.auth_spray_rows_marked,
            auth_spray_ip_count=result.auth_spray_ip_count,
            ip_churn_rows_marked=result.ip_churn_rows_marked,
            ip_churn_ip_count=result.ip_churn_ip_count,
            network_churn_rows_marked=result.network_churn_rows_marked,
            network_churn_prefix_count=result.network_churn_prefix_count,
            distributed_proxy_rows_marked=result.distributed_proxy_rows_marked,
            distributed_proxy_prefix_count=result.distributed_proxy_prefix_count,
            observed_cloud_rows_marked=result.observed_cloud_rows_marked,
            observed_cloud_prefix_count=result.observed_cloud_prefix_count,
        )
    )
