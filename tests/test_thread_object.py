import os
import sys
import threading
if True:
    # for Hydrogen
    if False:
        __file__ = "./test_thread_object.py"
    sys.path.insert(0, os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"
    ))
    from threaded_object import DedicatedThreadClass, run_on_dedicated_thread

thr = threading.current_thread()
thr.ident


class TestClass(DedicatedThreadClass):
    @run_on_dedicated_thread
    def add(self, a, b):
        return (a+b, threading.current_thread().ident)


test_obj = TestClass()
sum, thread_id = test_obj.add(2, 3)
print(sum)
test_obj.terminate()
assert sum == 5 and thread_id != threading.current_thread().ident
