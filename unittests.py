import unittest
from unittest import TestCase
from queue_task import *
from performance_test import task_generator
from copy import copy


class TestResourcesClass(TestCase):
    def test_resources_comparison(self):
        r_ram1 = Resources(ram=1, cpu_cores=1, gpu_count=1)
        r_ram2 = Resources(ram=2, cpu_cores=1, gpu_count=1)
        self.assertTrue(r_ram1<=r_ram2)
        self.assertTrue(r_ram1<=r_ram1)
        self.assertFalse(r_ram2<=r_ram1)

        r_cpu2 = Resources(ram=1, cpu_cores=2, gpu_count=1)
        self.assertTrue(r_ram1 <= r_cpu2)
        self.assertFalse(r_cpu2 <= r_ram1)

        r_gpu2 = Resources(ram=1, cpu_cores=1, gpu_count=2)
        self.assertTrue(r_ram1 <= r_gpu2)
        self.assertFalse(r_gpu2 <= r_ram1)

class TestTaskQueue_dict_of_dicts(TestCase):

    def create_tasks(self, n) -> list[Task]:
        small_resoure_spec = dict(
            ram=lambda: 5,
            cpu=lambda: 1,
            gpu=lambda: 1
        )
        priorityfnc = lambda: 2
        gen = task_generator(small_resoure_spec, priorityfnc)
        return [next(gen) for i in range(n)]

    def test_add_task(self):
        tasks = self.create_tasks(5)
        tasks[1].priority = 1
        queue = TaskQueue_dict_of_dicts()
        for task in tasks:
            queue.add_task(task)

        self.assertEqual(len(queue.storage),2)
        self.assertEqual(len(queue.storage[2]),4)
        self.assertEqual(list(queue.storage[1].values()), [tasks[1]])
        del tasks[1]
        self.assertEqual(list(queue.storage[2].values()), tasks)

    def test_get_task(self):
        tasks = self.create_tasks(5)
        tasks[2].priority = 1
        queue = TaskQueue_dict_of_dicts()
        for task in tasks:
            queue.add_task(task)

        consum_res = copy(tasks[0].resources)
        self.assertEqual(queue.get_task(consum_res), tasks[2])
        self.assertEqual(queue.get_task(consum_res), tasks[0])
        self.assertEqual(len(queue.storage[2]), 3)
        self.assertEqual(len(queue.storage[1]), 0)
        consum_res.ram = 1
        self.assertEqual(queue.get_task(consum_res), None)
        self.assertEqual(len(queue.storage[2]), 3)


if __name__ == '__main__':
    unittest.main()