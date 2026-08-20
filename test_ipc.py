import os
import time

from ipc import (
    Order,
    BUY,
    SELL,
    RingBuffer,
)


IPC_FILE = "test_market-firhose-mmap.ipc"


def main():

    # Remove previous test file
    if os.path.exists(IPC_FILE):
        os.remove(IPC_FILE)

    # -------------------------
    # Create producer buffer
    # -------------------------

    producer = RingBuffer(
        path=IPC_FILE,
        capacity=100,
    )

    producer.create()

    # -------------------------
    # Write orders
    # -------------------------

    print("Writing orders...")

    for i in range(1, 11):

        order = Order(
            order_id=i,
            price=245.50 + i,
            quantity=100 + i,
            side=BUY if i % 2 == 0 else SELL,
            timestamp_ns=time.perf_counter_ns(),
        )

        success = producer.write(order)

        if not success:
            print("Buffer full!")

    # -------------------------
    # Consumer opens same mmap
    # -------------------------

    consumer = RingBuffer(
        path=IPC_FILE,
        capacity=100,
    )

    consumer.open()

    print("\nReading orders...\n")

    while not consumer.is_empty():

        order = consumer.read()

        print(
            f"ID={order.order_id} "
            f"Side={'BUY' if order.side == BUY else 'SELL'} "
            f"Price={order.price:.2f} "
            f"Quantity={order.quantity}"
        )

    producer.close()
    consumer.close()

    os.remove(IPC_FILE)

    print("\nIPC test completed successfully.")


if __name__ == "__main__":
    main()
