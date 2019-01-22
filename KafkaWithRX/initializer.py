import multiprocessing
import rx

from multiprocessing import Process
from concurrent.futures import ProcessPoolExecutor


def none(msg):
    pass


# def process_initializer(iterable, consumer):
def worker(consumer, iterable):
    num_cores = multiprocessing.cpu_count()
    with ProcessPoolExecutor(num_cores) as executor:
        rx.Observable.from_(iterable).flat_map(
            lambda msg: executor.submit(consumer.process_consumer, msg)
        ).subscribe(consumer)


class RXInitializer:

    def __init__(self, consumers):
        self.__processes = self.__get_processes(consumers)

    def __get_processes(self, consumers):
        return [
            Process(target=worker, args=(consumer, consumer.iterable))
            for consumer in consumers
        ]

    def run(self):
        [
            process.start()
            for process in self.__processes
        ]

    def release(self):
        return [
            process.join()
            for process in self.__processes
        ]
