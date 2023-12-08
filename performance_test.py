from queue_task import *
from random import randint
from statistics import mean, stdev
from math import sqrt
import itertools
import time

genid = itertools.count()  # Infinite source of integers for Task ids


def get_resources(spec: dict):
    """Make a Resource object."""
    return Resources(
        ram=spec['ram'](),
        cpu_cores=spec['cpu'](),
        gpu_count=spec['gpu']()
    )


def task_generator(resoure_spec, priorityfnc):
    """Yield a Task object"""
    while True:
        yield Task(
            id=next(genid),
            priority=priorityfnc(),
            resources=get_resources(resoure_spec),
            content='',
            result=''
        )


def testcase_random_sample(ntasks: int, nattempts: int):
    """General case.

    Tasks and clients have random amounts of resources. The priorities
    are also random.
    :param ntasks: number of tasks to put into the queue
    :param nattempts: number of attempts for testing
    """

    random_resoure_spec = dict(
        ram=lambda: randint(1, 500),
        cpu=lambda: randint(1, 10),
        gpu=lambda: randint(1, 10)
    )
    priorityfnc = lambda: randint(1, 5)  # Generate priorities from 1 to 10
    taskgen = task_generator(random_resoure_spec, priorityfnc)  # Initialize the infinite generator
    # Get the required number of elements and save them to a list to be able
    # to repeat the test with different TaskQueue classes
    tasklist = list(itertools.islice(taskgen, 0, ntasks))  # Slice with ntasks elements
    # Also save consumers resources to test the classes in the same conditions
    consres_initial_list = [get_resources(random_resoure_spec) for i in range(nattempts*2)]

    for cls in (TaskQueue_list, TaskQueue_dict_of_lists, TaskQueue_dict_of_dicts):
        times_add, times_get = [], []
        consumer_resources = consres_initial_list.copy()
        while len(times_get) < nattempts:
            queue = cls()   # Create a new Queue instance
            tstart_add = time.time()
            list(map(queue.add_task, tasklist))   # fill it
            tend_add = time.time()
            if len(consumer_resources) == 0:
                raise Exception("List of consumer resources has been exhausted. "
                                "Please, run the test again.")
            tstart_get = time.time()
            task = queue.get_task(consumer_resources.pop())   # do job
            tend_get = time.time()
            print(f'Got {task}')
            if task is not None:
                times_add.append(tend_add - tstart_add)
                times_get.append(tend_get - tstart_get)
        print(f'Timing for {cls.__name__}:')
        print('  Add: {:1.3e} +\- {:1.3e} sec ({:d} calls)'.format(
            mean(times_add), stdev(times_add)/sqrt(nattempts), ntasks))
        print('  Get: {:1.6f} +\- {:1.6f} sec (per call)'.format(
            mean(times_get), stdev(times_get)/sqrt(nattempts)))


def testcase_appropriate_task_at_the_end(ntasks: int, nattempts: int):
    """The worst case: the appropriate task is at the end of the queue

    There is only one task that satisfies the consumer resources.
    We have to path through all the queue
    :param ntasks: number of tasks to put into the queue
    :param nattempts: number of attempts for testing
    """

    big_resoure_spec = dict(
        ram=lambda: 500,
        cpu=lambda: 10,
        gpu=lambda: 10
    )

    small_resoure_spec = dict(
        ram=lambda: 5,
        cpu=lambda: 1,
        gpu=lambda: 1
    )
    priorityfnc = lambda: 2  # High priority tasks first
    taskgen_big = task_generator(big_resoure_spec, priorityfnc)  # Initialize the infinite generator
    # Get the required number of elements and save them to a list to be able
    # to repeat the test with different TaskQueue classes
    tasklist = list(itertools.islice(taskgen_big, 0, ntasks))  # Slice with ntasks elements
    # Also save consumer resources to test the classes in the same conditions
    consres_initial_list = [get_resources(small_resoure_spec) for i in range(nattempts*2)]

    # Add one more task (the appropriate one) to the end
    taskgen_small = task_generator(small_resoure_spec, lambda: 2)
    tasklist.append(next(taskgen_small))

    for cls in (TaskQueue_list, TaskQueue_dict_of_lists, TaskQueue_dict_of_dicts):
        times_add, times_get = [], []
        consumer_resources = consres_initial_list.copy()
        while len(times_get) < nattempts:
            queue = cls()   # Create a new Queue instance
            tstart_add = time.time()
            list(map(queue.add_task, tasklist))   # fill it
            tend_add = time.time()
            if len(consumer_resources) == 0:
                raise Exception("List of consumer resources has been exhausted. "
                                "Please, run the test again.")
            tstart_get = time.time()
            task = queue.get_task(consumer_resources.pop())   # do job
            tend_get = time.time()
            print(f'Got {task}')
            if task is not None:
                times_add.append(tend_add - tstart_add)
                times_get.append(tend_get - tstart_get)
        print(f'Timing for {cls.__name__}:')
        print('  Add: {:1.3e} +\- {:1.3e} sec ({:d} calls)'.format(
            mean(times_add), stdev(times_add)/sqrt(nattempts), ntasks))
        print('  Get: {:1.6f} +\- {:1.6f} sec (per call)'.format(
            mean(times_get), stdev(times_get)/sqrt(nattempts)))



def _main():
    # testcase_random_sample(ntasks=5000000, nattempts=10)
    # testcase_appropriate_task_at_the_end(ntasks=5000000, nattempts=10)
    testcase_random_sample(ntasks=10000, nattempts=10)
    testcase_appropriate_task_at_the_end(ntasks=10000, nattempts=10)


if __name__ == '__main__':
    _main()