"""
ChronosMatch End-to-End IPC Consumer.

Reads orders from the mmap ring buffer and
measures producer-to-consumer latency.
"""

import os
import time

from ipc import RingBuffer


IPC_FILE = "e2e_chronosmatch.ipc"

BUFFER_CAPACITY = 100_000


def percentile(values, percentile_value):
    """
    Calculate a percentile from a sorted list.
    """

    if not values:
        return 0

    index = int(
        len(values) * percentile_value / 100
    )

    index = min(
        index,
        len(values) - 1
    )

    return values[index]


def wait_for_ipc():
    """Wait until the producer creates the IPC file."""

    print("Waiting for IPC buffer...")

    while not os.path.exists(IPC_FILE):
        time.sleep(0.1)

    print("IPC buffer detected.")


def run_consumer():

    wait_for_ipc()

    ring_buffer = RingBuffer(
        path=IPC_FILE,
        capacity=BUFFER_CAPACITY,
    )

    ring_buffer.open()

    expected = ring_buffer.get_expected_orders()

    print("=" * 65)
    print("ChronosMatch E2E Consumer")
    print("=" * 65)

    print(
        f"Expected orders : {expected:,}"
    )

    print(
        "Latency measurement: ENABLED"
    )

    print()

    start_time = time.perf_counter()

    consumed = 0

    latencies_ns = []

    last_display = start_time

    while True:

        order = ring_buffer.read()

        if order is not None:

            # Timestamp when the consumer receives
            # the order from the mmap buffer.
            receive_time_ns = (
                time.perf_counter_ns()
            )

            # Producer timestamp → Consumer timestamp
            latency_ns = (
                receive_time_ns
                - order.timestamp_ns
            )

            # Ignore impossible negative values.
            if latency_ns >= 0:
                latencies_ns.append(
                    latency_ns
                )

            consumed += 1

        else:

            # Producer has finished AND
            # all orders have been consumed.
            if (
                ring_buffer.is_producer_done()
                and consumed >= expected
            ):
                break

            time.sleep(0)

        now = time.perf_counter()

        # Display progress every second.
        if now - last_display >= 1:

            elapsed = (
                now - start_time
            )

            rate = (
                consumed / elapsed
                if elapsed > 0
                else 0
            )

            print(
                f"[Consumer] "
                f"{consumed:,}/{expected:,} "
                f"orders | "
                f"{rate:,.0f} orders/sec"
            )

            last_display = now

    elapsed = (
        time.perf_counter()
        - start_time
    )

    throughput = (
        consumed / elapsed
        if elapsed > 0
        else 0
    )

    # ========================================================
    # LATENCY CALCULATIONS
    # ========================================================

    latencies_ns.sort()

    if latencies_ns:

        min_latency_ns = (
            latencies_ns[0]
        )

        max_latency_ns = (
            latencies_ns[-1]
        )

        average_latency_ns = (
            sum(latencies_ns)
            / len(latencies_ns)
        )

        p50_ns = percentile(
            latencies_ns,
            50
        )

        p95_ns = percentile(
            latencies_ns,
            95
        )

        p99_ns = percentile(
            latencies_ns,
            99
        )

        p999_ns = percentile(
            latencies_ns,
            99.9
        )

    else:

        min_latency_ns = 0
        max_latency_ns = 0
        average_latency_ns = 0
        p50_ns = 0
        p95_ns = 0
        p99_ns = 0
        p999_ns = 0

    # Convert nanoseconds → microseconds.
    min_latency_us = (
        min_latency_ns / 1_000
    )

    average_latency_us = (
        average_latency_ns / 1_000
    )

    p50_us = p50_ns / 1_000
    p95_us = p95_ns / 1_000
    p99_us = p99_ns / 1_000
    p999_us = p999_ns / 1_000

    max_latency_us = (
        max_latency_ns / 1_000
    )

    # ========================================================
    # FINAL RESULTS
    # ========================================================

    print()
    print("=" * 65)
    print("Consumer Finished")
    print("=" * 65)

    print(
        f"Orders consumed : {consumed:,}"
    )

    print(
        f"Expected        : {expected:,}"
    )

    print(
        f"Orders missing  : "
        f"{expected - consumed:,}"
    )

    print(
        f"Elapsed time    : "
        f"{elapsed:.6f} sec"
    )

    print(
        f"Throughput      : "
        f"{throughput:,.0f} orders/sec"
    )

    print()
    print("-" * 65)
    print("IPC LATENCY")
    print("-" * 65)

    print(
        f"Samples         : "
        f"{len(latencies_ns):,}"
    )

    print(
        f"Minimum         : "
        f"{min_latency_us:.3f} µs"
    )

    print(
        f"Average         : "
        f"{average_latency_us:.3f} µs"
    )

    print(
        f"P50             : "
        f"{p50_us:.3f} µs"
    )

    print(
        f"P95             : "
        f"{p95_us:.3f} µs"
    )

    print(
        f"P99             : "
        f"{p99_us:.3f} µs"
    )

    print(
        f"P99.9           : "
        f"{p999_us:.3f} µs"
    )

    print(
        f"Maximum         : "
        f"{max_latency_us:.3f} µs"
    )

    print("-" * 65)
    print()

    ring_buffer.close()

    # Cleanup IPC file.
    if os.path.exists(IPC_FILE):

        try:
            os.remove(IPC_FILE)

        except PermissionError:

            print(
                "Warning: Could not remove IPC file."
            )


if __name__ == "__main__":
    run_consumer()
