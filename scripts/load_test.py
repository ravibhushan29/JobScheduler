"""
Load test: how many jobs per second the stack accepts and completes.

    python scripts/load_test.py --api http://localhost:8000 --api http://localhost:8001 \\
        --key k --jobs 5000 --concurrency 100

Submits `--jobs` add(x, y) jobs through every API replica (round-robin) with
`--concurrency` requests in flight, then waits until every one has finished.
Reports submit throughput and latency (p50/p95/p99), end-to-end completion
throughput, and checks that each job ran exactly once (attempts == 1).

Only needs httpx (in requirements.txt).
"""

import argparse
import asyncio
import statistics
import sys
import time
import uuid

import httpx


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(int(q * len(ordered)), len(ordered) - 1)]


async def main(args: argparse.Namespace) -> int:
    headers = {"X-API-Key": args.key} if args.key else {}
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    clients = [httpx.AsyncClient(base_url=api, headers=headers, timeout=30, limits=limits) for api in args.api]
    run_tag = uuid.uuid4().hex[:8]
    latencies: list[float] = []
    task_ids: list[str] = []
    errors = 0
    error_kinds: dict[str, int] = {}
    semaphore = asyncio.Semaphore(args.concurrency)

    retries = 0

    async def submit(i: int) -> None:
        """One job. Connection errors are retried with the SAME Idempotency-Key, like the SDKs do."""
        nonlocal errors, retries
        client = clients[i % len(clients)]
        key = f"load-{run_tag}-{i}"
        async with semaphore:
            started = time.perf_counter()
            for attempt in range(3):
                try:
                    response = await client.post(
                        "/v1/jobs",
                        json={"task_name": "app.tasks.add", "args": [i, 1], "priority": args.priority},
                        headers={"Idempotency-Key": key},
                    )
                    response.raise_for_status()
                    task_ids.append(response.json()["task_id"])
                    latencies.append(time.perf_counter() - started)
                    return
                except httpx.TransportError as exc:
                    if attempt == 2:
                        kind = type(exc).__name__
                        break
                    retries += 1
                except httpx.HTTPStatusError as exc:
                    kind = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
                    break
            errors += 1
            error_kinds[kind] = error_kinds.get(kind, 0) + 1

    print(f"Submitting {args.jobs} jobs via {len(clients)} API replica(s), {args.concurrency} in flight…", flush=True)
    t0 = time.perf_counter()
    await asyncio.gather(*(submit(i) for i in range(args.jobs)))
    submit_seconds = time.perf_counter() - t0

    # Wait for completion: page through the job records of this run.
    pending = set(task_ids)
    deadline = time.perf_counter() + args.timeout
    attempts: dict[str, int] = {}
    while pending and time.perf_counter() < deadline:
        await asyncio.sleep(1)
        ids = list(pending)

        async def check(task_id: str) -> None:
            async with semaphore:
                job = (await clients[0].get(f"/v1/jobs/{task_id}")).json()
                if job["status"] in ("SUCCESS", "FAILURE", "REVOKED"):
                    attempts[task_id] = job["attempts"]
                    pending.discard(task_id)

        # Checking every id each second is itself load; sample the oldest ones until they're done.
        await asyncio.gather(*(check(t) for t in ids[: args.concurrency * 4]))
    total_seconds = time.perf_counter() - t0
    for client in clients:
        await client.aclose()

    done = len(task_ids) - len(pending)
    print()
    print(
        f"submitted      {len(task_ids)} ok, {errors} errors in {submit_seconds:.1f} s "
        f"-> {len(task_ids) / submit_seconds:.0f} submits/s"
    )
    print(f"retried        {retries} connection errors (same Idempotency-Key, so no duplicates)")
    for kind, count in error_kinds.items():
        print(f"  error x{count}: {kind}")
    if latencies:
        print(
            f"submit latency p50 {pct(latencies, 0.5) * 1000:.0f} ms  p95 {pct(latencies, 0.95) * 1000:.0f} ms  "
            f"p99 {pct(latencies, 0.99) * 1000:.0f} ms  mean {statistics.mean(latencies) * 1000:.0f} ms"
        )
    print(
        f"completed      {done}/{len(task_ids)} in {total_seconds:.1f} s from first submit "
        f"-> {done / total_seconds:.0f} jobs/s end to end"
    )
    reruns = sum(1 for a in attempts.values() if a != 1)
    print(f"exactly once   {len(attempts) - reruns}/{len(attempts)} finished jobs ran exactly once")
    duplicates = len(task_ids) - len(set(task_ids))
    print(f"duplicates     {duplicates} (two submissions that became two jobs)")
    return 0 if not pending and not errors and not reruns and not duplicates else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--api", action="append", required=True, help="API base URL (repeat for each replica)")
    parser.add_argument("--key", default="", help="X-API-Key")
    parser.add_argument("--jobs", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--priority", default="normal", choices=["high", "normal", "low"])
    parser.add_argument("--timeout", type=float, default=600, help="seconds to wait for completion")
    sys.exit(asyncio.run(main(parser.parse_args())))
