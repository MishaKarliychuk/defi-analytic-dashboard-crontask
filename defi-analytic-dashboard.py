import asyncio
import logging
import os
import sys
from typing import Any

import aiohttp

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "r0RlYmtlgHD4E7HL2kwHIsvdn7i92dNa")
BASE_URL = "https://api.dune.com/api/v1"

POLL_INTERVAL_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 60
DELAY_BETWEEN_QUERIES_SECONDS = 5*60

TERMINAL_SUCCESS_STATES = {"QUERY_STATE_COMPLETED"}
TERMINAL_FAILURE_STATES = {
    "QUERY_STATE_FAILED",
    "QUERY_STATE_CANCELLED",
    "QUERY_STATE_EXPIRED",
}

# Groups run in parallel. Queries inside a group run sequentially in the
# order defined by the dict (Python 3.7+ preserves insertion order).
# Format: {query_id: materialized_view_name}
QUERY_GROUPS: list[dict[str, str]] = [
    # DEF1
    {
        "6750819": "dune.dt_team.result_main_query_misha_contract_part_1",
        "6540846": "dune.dt_team.result_main_query_misha_contract",
        "6631964": "dune.dt_team.result_main_solana_query_misha_wallet",
        "6793626": "dune.dt_team.result_main_solana_query_misha_wallet_part_2",
        "6614359": "dune.dt_team.result_matching_query_misha",
    },
    # FCC
    {
        "6958092": "dune.dt_team.result_evm_query_fcc_contract_part_1",
        "6968761": "dune.dt_team.result_solana_query_fcc_wallet_part_1",
        "6969192": "dune.dt_team.result_evm_query_fcc_contract_part_2",
    },
    # 70C
    {
        "6972746": "dune.dt_team.result_solana_query_70c_wallet_part_1",
        "6972233": "dune.dt_team.result_evm_query_70c_wallet_part_1",
        "6973378": "dune.dt_team.result_70c_matching_query",
    },
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dune-refresh")


class DuneError(RuntimeError):
    pass


async def _post(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    async with session.post(url) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise DuneError(f"POST {url} -> {resp.status}: {text}")
        return await resp.json(content_type=None) if text else {}


async def _get(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    async with session.get(url) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise DuneError(f"GET {url} -> {resp.status}: {text}")
        return await resp.json(content_type=None) if text else {}


async def execute_query(
    session: aiohttp.ClientSession, query_id: str
) -> str:
    url = f"{BASE_URL}/query/{query_id}/execute"
    data = await _post(session, url)
    execution_id = data.get("execution_id")
    if not execution_id:
        raise DuneError(f"No execution_id in response for query {query_id}: {data}")
    return execution_id


async def refresh_materialized_view(
    session: aiohttp.ClientSession, view_name: str
) -> str | None:
    url = f"{BASE_URL}/materialized-views/{view_name}/refresh"
    data = await _post(session, url)
    return data.get("execution_id")


async def wait_for_execution(
    session: aiohttp.ClientSession,
    execution_id: str,
    label: str,
) -> dict[str, Any]:
    url = f"{BASE_URL}/execution/{execution_id}/status"
    while True:
        data = await _get(session, url)
        state = data.get("state")
        if state in TERMINAL_SUCCESS_STATES:
            log.info("[%s] execution %s completed", label, execution_id)
            return data
        if state in TERMINAL_FAILURE_STATES:
            raise DuneError(
                f"[{label}] execution {execution_id} ended with state {state}: {data}"
            )
        log.info("[%s] execution %s state=%s, waiting...", label, execution_id, state)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def process_group(
    group_idx: int, group: dict[str, str]
) -> tuple[int, bool, str | None]:
    """Process one group. Never raises — failures are returned as (idx, False, err)."""
    label = f"group-{group_idx}"
    log.info("[%s] starting, %d queries", label, len(group))

    headers = {"X-Dune-Api-Key": DUNE_API_KEY}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    try:
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            total = len(group)
            for step_idx, (query_id, view_name) in enumerate(group.items(), start=1):
                step_label = f"{label}/step-{step_idx}/query-{query_id}"
                log.info("[%s] executing query", step_label)

                exec_id = await execute_query(session, query_id)
                await wait_for_execution(session, exec_id, step_label)

                log.info("[%s] refreshing view %s", step_label, view_name)
                refresh_exec_id = await refresh_materialized_view(session, view_name)
                if refresh_exec_id:
                    await wait_for_execution(
                        session, refresh_exec_id, f"{step_label}/refresh"
                    )
                else:
                    log.info(
                        "[%s] refresh response had no execution_id, assuming sync refresh",
                        step_label,
                    )

                if step_idx < total:
                    log.info(
                        "[%s] sleeping %ds before next query",
                        step_label,
                        DELAY_BETWEEN_QUERIES_SECONDS,
                    )
                    await asyncio.sleep(DELAY_BETWEEN_QUERIES_SECONDS)
    except asyncio.CancelledError:
        raise
    except BaseException as err:
        log.error("[%s] FAILED: %s", label, err)
        return (group_idx, False, str(err))

    log.info("[%s] done", label)
    return (group_idx, True, None)


async def main() -> int:
    if not QUERY_GROUPS or all(not g for g in QUERY_GROUPS):
        log.error("QUERY_GROUPS is empty — fill in query_id -> materialized_view pairs")
        return 1

    tasks = [
        asyncio.create_task(process_group(i, g), name=f"group-{i}")
        for i, g in enumerate(QUERY_GROUPS)
        if g
    ]

    results = await asyncio.gather(*tasks)

    succeeded = [idx for idx, ok, _ in results if ok]
    failed = [(idx, err) for idx, ok, err in results if not ok]

    log.info(
        "summary: %d/%d groups succeeded, %d failed",
        len(succeeded),
        len(results),
        len(failed),
    )
    for idx, err in failed:
        log.error("group-%d failed: %s", idx, err)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
