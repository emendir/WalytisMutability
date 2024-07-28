import os
import sys
import threading
import _testing_utils
import mutablockchain
from mutablockchain.threaded_object import DedicatedThreadClass, run_on_dedicated_thread
_testing_utils.assert_is_loaded_from_source(
    source_dir=os.path.dirname(os.path.dirname(__file__)), module=mutablockchain
)

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
