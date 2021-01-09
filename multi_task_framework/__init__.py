from functools import partial
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor


class Framework:

    def __init__(self, start_generator, max_processes=10, max_threads=100):
        self.start_generator = start_generator() if callable(start_generator) else start_generator
        self.max_processes = max_processes
        self.max_threads = max_threads
        self.tasks = []
        self.results = []
        self.temp_results = []

    def add_task(self, task_func, *args, **kwargs):
        if len(self.tasks) > 1:
            raise Exception("There can't be more than 2 tasks!")
        self.tasks.append(partial(task_func, *args, **kwargs))

    def run_thread_task(self, task, ctrl_generator):
        task_pool = ThreadPoolExecutor(max_workers=self.max_threads)
        task_results = []
        for elem in ctrl_generator:
            task_pool.submit(task, elem).add_done_callback(lambda future: task_results.append(future.result()))
        task_pool.shutdown()
        return task_results

    def run_process_task(self, task, ctrl_generator, callback):
        task_pool = ProcessPoolExecutor(max_workers=self.max_processes)
        task_results = []
        for elem in ctrl_generator:
            task_pool.submit(task, elem).add_done_callback(lambda future: task_results.extend(list(callback(future.result()))))
        task_pool.shutdown()
        return task_results

    def run(self):
        if len(self.tasks) == 0:
            raise Exception('There is no task!')
        if len(self.tasks) == 1:
            self.results.extend(self.run_thread_task(self.tasks[0], self.start_generator))
        elif len(self.tasks) == 2:
            self.results.extend(self.run_process_task(
                self.tasks[0], self.start_generator,
                partial(self.run_thread_task, self.tasks[1])
            ))
        return self.results
