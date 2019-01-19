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


def rx_initializer(consumer):
    # gevent.spawn(worker(iterable, consumer)).join()
    Process(target=worker, args=(consumer, consumer.iterable)).start()
