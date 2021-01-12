# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import time
from __init__ import Framework


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def start_values():
    return range(10)


def task1(a):
    time.sleep(1)
    print(f'start task {a} ....\n')
    return list(map(lambda x: x * a, range(3000)))


def task2(b, a):
    time.sleep(1)
    print(f'processing task {a} thread {b} ...\n')
    return b + 100


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    framework = Framework(start_values())
    framework.add_base_task(task1)
    framework.add_sub_task(task2)
    result = framework.run()
    print(result)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
