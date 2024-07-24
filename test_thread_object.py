from threaded_object import DedicatedThreadClass, run_on_dedicated_thread
import threading

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
