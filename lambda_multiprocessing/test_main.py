import unittest
import multiprocessing
from lambda_multiprocessing import Pool, TimeoutError, AsyncResult, Worker
from time import time, sleep
from typing import Tuple
from pathlib import Path
import os

import boto3
from moto import mock_s3

# add an overhead for duration when asserting the duration of child processes
# if other processes are hogging CPU, make this bigger
delta = 0.1

def square(x):
    return x*x

def fail(x):
    assert False, "deliberate fail"

def sum_two(a, b):
    return a + b

def divide(a, b):
    return a / b

def return_args_kwargs(*args, **kwargs):
    return {'args': args, 'kwargs': kwargs}

# some simple functions to run inside the child process
class SquareWorker(Worker):
    def action(self, x):
        return x*x
class FailWorker(Worker):
    def action(self, x):
        assert False, "deliberate fail"
class SumTwoWorker(Worker):
    def action(self, a, b):
        return a + b
class DivideWorker(Worker):
    def action(self, a, b):
        return a / b

class ReturnArgsKwargsWorker(Worker):
    def action(self, *args, **kwargs):
        return {'args': args, 'kwargs': kwargs}

class SleepWorker(Worker):
    def action(self, x):
        return sleep(x)

class UploadWorker(Worker):
    def action(self, args: Tuple[str, str, bytes]):
        (bucket_name, key, data) = args
        client = boto3.client('s3')
        client.put_object(Bucket=bucket_name, Key=key, Body=data)
        sleep(1)
        return client.get_object(Bucket=bucket_name, Key=key)['Body'].read()

class TestStdLib(unittest.TestCase):
    @unittest.skip('Need to set up to remove /dev/shm')
    def test_standard_library(self):
        with self.assertRaises(OSError):
            with multiprocessing.Pool() as p:
                ret = p.map(square, range(5))

# add assertDuration
class TestCase(unittest.TestCase):
    # use like
    # with self.assertDuration(1, 2):
    #   something
    # to assert that something takes between 1 to 2 seconds to run
    def assertDuration(self, min_t=None, max_t=None):
        class AssertDuration:
            def __init__(self, test):
                self.test = test
            def __enter__(self):
                self.start_t = time()

            def __exit__(self, exc_type, exc_val, exc_tb):
                if exc_type is None:
                    end_t = time()
                    duration = end_t - self.start_t
                    if min_t is not None:
                        self.test.assertGreaterEqual(duration, min_t, f"Took less than {min_t}s to run")
                    if max_t is not None:
                        self.test.assertLessEqual(duration, max_t, f"Took more than {max_t}s to run")

        return AssertDuration(self)


class TestAssertDuration(TestCase):

    def test(self):
        # check that self.assertDurationWorks
        with self.assertRaises(AssertionError):
            with self.assertDuration(min_t=1):
                pass

        with self.assertRaises(AssertionError):
            with self.assertDuration(max_t=0.5):
                sleep(1)

        t = 0.5
        with self.assertDuration(min_t=t-delta, max_t=t + delta):
            sleep(t)

class TestApply(TestCase):

    def test_simple(self):
        args = range(5)
        with Pool(worker=SquareWorker) as p:
            actual = [p.apply((x,)) for x in args]
        with multiprocessing.Pool() as p:
            expected = [p.apply(square, (x,)) for x in args]
        self.assertEqual(actual, expected)

    def test_two_args(self):
        with Pool(worker=SumTwoWorker) as p:
            actual = p.apply(args = (3, 4))
            self.assertEqual(actual, sum_two(3, 4))

    def test_kwargs(self):
        with Pool(worker=ReturnArgsKwargsWorker) as p:
            actual = p.apply((1, 2), {'x': 'X', 'y': 'Y'})
            expected = {
                'args': (1,2),
                'kwargs': {
                    'x': 'X',
                    'y': 'Y'
                }
            }
            self.assertEqual(actual, expected)

class TestApplyAsync(TestCase):

    def test_result(self):
        with Pool(worker=SquareWorker) as p:
            r = p.apply_async((2,))
            result = r.get(2)
            self.assertEqual(result, square(2))

    def test_twice(self):
        args = range(5)
        # now try twice with the same pool
        with Pool(worker=SquareWorker) as p:
            ret = [p.apply_async((x,)) for x in args]
            ret.extend(p.apply_async((x,)) for x in args)

        self.assertEqual([square(x) for x in args] * 2, [square(x) for x in args] * 2)

    def test_time(self):
        # test the timing to confirm it's in parallel
        n = 4
        with Pool(n, worker=SleepWorker) as p:
            with self.assertDuration(min_t=n-1, max_t=(n-1)+delta):
                with self.assertDuration(max_t=1):
                    ret = [p.apply_async((r,)) for r in range(n)]
                ret = [r.get(n*2) for r in ret]

    @unittest.skip("Standard library doesn't handle this well, unsure whether to do the same or not")
    def test_unclean_exit(self):
        # .get after __exit__, but process finishes before __exit__
        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with Pool() as p:
                r = p.apply_async(square, (t,))
                sleep(1)
            r.get()

        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with Pool() as p:
                r = p.apply_async(square, (t,))
                sleep(1)
            r.get(1)

        # __exit__ before process finishes
        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with Pool() as p:
                r = p.apply_async(sleep, (t,))
            r.get(t+1) # .get() with arg after __exit__

        with self.assertRaises(multiprocessing.TimeoutError):
            t = 1
            with Pool() as p:
                r = p.apply_async(sleep, (t,))
            r.get() # .get() without arg after __exit__

    def test_get_not_ready_a(self):
        t = 2
        with Pool(worker=SleepWorker) as p:
            r = p.apply_async((t,))
            with self.assertRaises(multiprocessing.TimeoutError):
                r.get(t-1) # result not ready get

    def test_get_not_ready_b(self):
        t = 2
        with Pool(worker=SleepWorker) as p:
            # check same exception exists from main
            r = p.apply_async((t,))
            with self.assertRaises(TimeoutError):
                r.get(t-1)

    def test_get_not_ready_c(self):
        t = 2
        with Pool(worker=SleepWorker) as p:
            r = p.apply_async((t,))
            sleep(1)
            self.assertFalse(r.ready())
            sleep(t)
            self.assertTrue(r.ready())

    def test_wait(self):
        with Pool(worker=SquareWorker) as p:
            r = p.apply_async((1,))
            with self.assertDuration(max_t=delta):
                r.wait()
            self.assertTrue(r.ready())
            ret = r.get(0)
            self.assertEqual(ret, square(1))

        with Pool(worker=SleepWorker) as p:
            # now check when not ready
            r = p.apply_async((5,))
            self.assertFalse(r.ready())
            with self.assertDuration(min_t=2, max_t=2.1):
                r.wait(2)
            self.assertFalse(r.ready())

            # now check with a wait longer than the task
            with self.assertDuration(min_t=1, max_t=1.1):
                r = p.apply_async((1,))
                self.assertFalse(r.ready())
                r.wait(2)
            self.assertTrue(r.ready())
            ret = r.get(0)

    def test_get_twice(self):
        with Pool(worker=SquareWorker) as p:
            r = p.apply_async((2,))
            self.assertEqual(r.get(), square(2))
            self.assertEqual(r.get(), square(2))

    def test_successful(self):
        with Pool(worker=SquareWorker) as p:
            r = p.apply_async((1,))
            sleep(delta)
            self.assertTrue(r.successful())

        with Pool(worker=SleepWorker) as p:
            r = p.apply_async((1,))
            with self.assertRaises(ValueError):
                r.successful()

            sleep(1.2)
            self.assertTrue(r.successful())

        with Pool(worker=FailWorker) as p:
            r = p.apply_async((1,))
            sleep(delta)
            self.assertFalse(r.successful())

    def test_two_args(self):
        with Pool(worker=SumTwoWorker) as p:
            ret = p.apply_async((1, 2))
            ret.wait()
            self.assertEqual(ret.get(), sum_two(1,2))

    def test_kwargs(self):
        with Pool(worker=ReturnArgsKwargsWorker) as p:
            actual = p.apply_async((1, 2), {'x': 'X', 'y': 'Y'}).get()
            expected = {
                'args': (1,2),
                'kwargs': {
                    'x': 'X',
                    'y': 'Y'
                }
            }
            self.assertEqual(actual, expected)

    def test_error_handling(self):
        with self.assertRaises(AssertionError):
            with Pool(worker=FailWorker) as p:
                r = p.apply_async((1,))
                r.get()

class TestSchedule(TestCase):
    def test_simple(self):
        args = range(5)
        with Pool(worker=SquareWorker) as p:
            actual = p.schedule(args)
        self.assertIsInstance(actual, list)
        expected = [square(x) for x in args]
        self.assertEqual(expected, actual)
        self.assertIsInstance(actual, list)

    def test_duration(self):
        n = 2
        with Pool(n, worker=SleepWorker) as p:
            with self.assertDuration(min_t=(n-1)-delta, max_t=(n+1)+delta):
                p.schedule(range(n))

    def test_error_handling(self):
        with Pool(worker=FailWorker) as p:
            with self.assertRaises(AssertionError):
                p.schedule(range(2))

    @unittest.skip('Need to implement chunking to fix this')
    def test_long_iter(self):
        with Pool(worker=SquareWorker) as p:
            p.schedule(range(10**3))

class TestTidyUp(TestCase):
    # test that the implicit __exit__
    # waits for child process to finish
    def test_exit(self):
        t = 1
        with Pool(worker=SleepWorker) as p:
            r = p.apply_async((t,))
            t1 = time()
        t2 = time()
        self.assertLessEqual(abs((t2-t1)-t), delta)

    # test that .close() stops new submisssions
    # but does not halt existing num_processes
    # nor wait for them to finish
    def test_close(self):
        t = 1
        with Pool(worker=SleepWorker) as p:
            r = p.apply_async((t,))
            with self.assertDuration(max_t=delta):
                p.close()
            with self.assertDuration(min_t=t-delta, max_t=t+delta):
                p.join()
            pass # makes traceback from __exit__ clearer

    def test_submit_after_close(self):
        with Pool(worker=SquareWorker) as p:
            p.close()
            with self.assertRaises(ValueError):
                p.apply_async((1,))

    # test that .terminate does not
    # wait for child process to finish
    def test_terminate(self):
        with self.assertDuration(max_t=delta):
            with Pool(worker=SleepWorker) as p:
                r = p.apply_async((1,))
                t1 = time()
                p.terminate()
                t2 = time()
                self.assertLessEqual(t2-t1, delta)

    def test_submit_after_terminate(self):
        with Pool(worker=SquareWorker) as p:
            p.terminate()
            with self.assertRaises(ValueError):
                p.apply_async((1,))



class TestMoto(TestCase):

    @mock_s3
    def test_moto(self):
        bucket_name = 'mybucket'
        key = 'my-file'
        data = b"123"
        client = boto3.client('s3')
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'ap-southeast-2'
            },
        )
        # upload in a different thread
        # when we create the other process,
        # everything in moto is duplicated (e.g. existing bucket)
        # so the function should execute correctly
        # but it will upload an object to mocked S3 in the child process
        # so that file won't exist in the parent process
        with Pool(worker=UploadWorker) as p:
            ret = p.apply(((bucket_name,key, data),))
        print(ret)
        self.assertEqual(ret, data)

class TestSlow(TestCase):
    @unittest.skip('Very slow')
    def test_memory_leak(self):
        for i in range(10**2):
            with Pool(worker=SquareWorker) as p:
                for j in range(10**2):
                    p.map(range(10**3))

if __name__ == '__main__':
    unittest.main()
