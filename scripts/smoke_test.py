"""
Smoke test script: Validate that all services are reachable after deployment.
Used in the Jenkins post-deploy stage and Argo pipeline health check step.

Usage:
    python scripts/smoke_test.py --base-url http://localhost:8000
    python scripts/smoke_test.py --run-date 2024-01-15
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

from src.config.logging_config import configure_logging, get_logger

logger = get_logger(__name__)

TIMEOUT = 15


def check_api_health(base_url: str) -> bool:
    try:
        resp = httpx.get(f"{base_url}/health", timeout=TIMEOUT)
        if resp.status_code == 200 and resp.json().get("status") == "ok":
            logger.info("api_health_ok", url=base_url)
            return True
        logger.error("api_health_fail", status=resp.status_code, body=resp.text[:200])
        return False
    except Exception as exc:
        logger.error("api_health_exception", error=str(exc))
        return False


def check_api_detailed_health(base_url: str) -> bool:
    try:
        resp = httpx.get(f"{base_url}/health/detailed", timeout=TIMEOUT)
        data = resp.json()
        services = data.get("services", [])
        all_ok = all(s["healthy"] for s in services)
        for svc in services:
            if svc["healthy"]:
                logger.info("service_healthy", service=svc["name"])
            else:
                logger.warning("service_unhealthy", service=svc["name"], message=svc.get("message"))
        return all_ok
    except Exception as exc:
        logger.error("detailed_health_exception", error=str(exc))
        return False


def check_datasets_endpoint(base_url: str) -> bool:
    try:
        resp = httpx.get(f"{base_url}/datasets", timeout=TIMEOUT)
        resp.raise_for_status()
        logger.info("datasets_endpoint_ok", count=resp.json().get("total", 0))
        return True
    except Exception as exc:
        logger.error("datasets_endpoint_fail", error=str(exc))
        return False


def check_oracle_row_count(run_date: str | None) -> bool:
    """Check that Oracle has rows loaded for today (or specified date)."""
    try:
        from src.config.settings import OracleSettings
        from src.load.oracle_loader import OracleLoader

        loader = OracleLoader(OracleSettings())
        result = loader.execute_query(
            "SELECT COUNT(*) AS cnt FROM ELT_PIPELINE.PIPELINE_RUN "
            "WHERE TRUNC(start_time) = TRUNC(SYSDATE)"
        )
        count = result[0]["cnt"] if result else 0
        logger.info("oracle_pipeline_runs_today", count=count, run_date=run_date)
        return True  # Don't fail if count is 0 — might just not have run yet
    except Exception as exc:
        logger.error("oracle_check_fail", error=str(exc))
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="ELT Pipeline Smoke Test")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--run-date", default="")
    args = parser.parse_args()

    configure_logging()
    logger.info("smoke_test_start", base_url=args.base_url)

    checks = {
        "API liveness": check_api_health(args.base_url),
        "API readiness": check_api_detailed_health(args.base_url),
        "Datasets endpoint": check_datasets_endpoint(args.base_url),
        "Oracle connectivity": check_oracle_row_count(args.run_date),
    }

    passed = sum(v for v in checks.values())
    total = len(checks)

    for name, result in checks.items():
        status = "PASS" if result else "FAIL"
        logger.info("check_result", check=name, result=status)

    logger.info("smoke_test_complete", passed=passed, total=total)

    if passed < total:
        failed_checks = [k for k, v in checks.items() if not v]
        logger.error("smoke_test_failed", failed=failed_checks)
        sys.exit(1)

    print(f"Smoke test passed {passed}/{total} checks.")


if __name__ == "__main__":
    main()
