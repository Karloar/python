from functools import partial
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


class Framework:

    def __init__(self, start_values, MAX_THREADS=100, MAX_PROCESSORS=10):
        self.start_values = start_values
        self.MAX_THREADS = MAX_THREADS
        self.MAX_PROCESSORS = MAX_PROCESSORS
        self.base_task = None
        self.base_task_tmp = None
        self.sub_task = None
        self.final_results = []

    def add_base_task(self, base_task, *args, **kwargs):
        if self.sub_task:
            raise Exception("Base task must not run after sub task!")
        self.base_task = partial(base_task, *args, **kwargs)
        self.base_task_tmp = self.base_task

    def add_sub_task(self, sub_task, *args, **kwargs):
        if not self.base_task:
            raise Exception("There is no base task!")
        self.sub_task = partial(sub_task, *args, **kwargs)
        self.base_task = self.new_base_task

    def new_base_task(self, base_task_key):
        params = self.base_task_tmp(base_task_key)
        return self.run_thread_task(self.sub_task, params, base_task_key)

    def run_thread_task(self, task, params, base_task_key):
        task_pool = ThreadPoolExecutor(max_workers=self.MAX_THREADS)
        task_results = []
        if self.sub_task:
            for elem in params:
                task_pool.submit(task, elem, base_task_key).add_done_callback(lambda future: task_results.append(future.result()))
        else:
            for elem in params:
                task_pool.submit(task, elem).add_done_callback(lambda future: task_results.append(future.result()))
        task_pool.shutdown()
        return task_results

    def run_process_task(self, task, params):
        task_pool = ProcessPoolExecutor(max_workers=self.MAX_PROCESSORS)
        task_results = []
        for elem in params:
            task_pool.submit(task, elem).add_done_callback(lambda future: task_results.extend(list(future.result())))
        task_pool.shutdown()
        return task_results

    def run(self):
        if not self.base_task:
            raise Exception("There is no base task!")
        if not self.sub_task:
            return self.run_thread_task(self.base_task, self.start_values, None)
        return self.run_process_task(self.base_task, self.start_values)
