from kafka import KafkaProducer
import queue
from concurrent.futures import ThreadPoolExecutor
import argparse


def get_args() -> argparse.Namespace:
    """
    Receive arguments from terminal
    :return: Namespace with arguments and its values
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threads", type=int, default=1)
    return parser.parse_args()


def process(que) -> None:
    """
    A function for parallel execution of several kafka producers.
        Each producer gets an element from queue and send it to
        the topic
    :param que: Common queue with data
    :return: None
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    while not que.empty():
        item = que.get()
        producer.send(topic, item)


if __name__ == "__main__":
    args = get_args()
    if args.threads <= 0:
        raise ValueError(
            f"Number of threads cannot be less or equal to 0. You passed {args.threads}"
        )

    data_path = "/tmp/train.csv"
    bootstrap_servers = ["sandbox-hdp.hortonworks.com:6667"]
    topic = "booking"
    q = queue.Queue()
    stop = 0
    with open(data_path, "rb") as data:
        for line in data:
            q.put(line)

    with ThreadPoolExecutor(args.threads) as executor:
        executor.submit(process, q)
