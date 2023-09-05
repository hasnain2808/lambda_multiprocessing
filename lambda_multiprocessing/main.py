from multiprocessing import TimeoutError, Process, Pipe
from multiprocessing.connection import Connection
from typing import Any, List, Dict, Tuple, Union
from uuid import uuid4, UUID
import random
import os
from time import time
import logging
import traceback

class Worker:
    proc: Process

    # this is the number of items that we have sent to the child process
    # minus the number we have received back
    # includes items in the queue not processed,
    # items currently being processed
    # and items that have been processed by the child, but not read by the parent
    # does not include the termination command from parent to child
    queue_sz: int = 0

    # parent_conn.send()  to give stuff to the child
    # parent_conn.recv() to get results back from child
    parent_conn: Connection
    child_conn: Connection

    result_cache: Dict[UUID, Tuple[Any, Exception]] = {}

    _closed: bool = False



    def __init__(self):
        self.parent_conn, self.child_conn = Pipe(duplex=True)
        self.proc = Process(target=self.spin)
        self.proc.start()
    # each child process runs in this
    # a while loop waiting for payloads from the self.child_conn
    # [(id, args, kwds), None] -> call action(args, *kwds)
    #                         and send the return back through the self.child_conn pipe
    #                         {id: (ret, None)} if action returned ret
    #                         {id: (None, err)} if action raised exception err
    # [None, True] -> exit gracefully (write nothing to the pipe)
    def spin(self) -> None:
        while True:
            (job, quit_signal) = self.child_conn.recv()
            if quit_signal:
                break
            else:
                (id, args, kwds) = job
                result = self._do_work(id, args, kwds)
                self.child_conn.send(result)
        self.child_conn.close()

    def _do_work(self, id, args, kwds) -> Union[Tuple[Any, None], Tuple[None, Exception]]:
        try:
            ret = {id: (self.action(*args, **kwds), None)}
        except Exception as e:
            print(f'\n\nMultiprocessing failed with error \n{traceback.format_exc()}\n\n')
            # how to handle KeyboardInterrupt?
            ret = {id: (None, e)}
        assert isinstance(list(ret.keys())[0], UUID)
        return ret

    def submit(self, args=(), kwds=None) -> 'AsyncResult':
        if self._closed:
            raise ValueError("Cannot submit tasks after closure")
        if kwds is None:
            kwds = {}
        id = uuid4()
        self.parent_conn.send([(id, args, kwds), None])
        self.queue_sz += 1
        return AsyncResult(id=id, child=self)

    # grab all results in the pipe from child to parent
    # save them to self.result_cache
    def flush(self):
        # watch out, when the other end is closed, a termination byte appears, so .poll() returns True
        while (not self.parent_conn.closed) and (self.queue_sz > 0) and self.parent_conn.poll(0):
            result = self.parent_conn.recv()
            assert isinstance(list(result.keys())[0], UUID)
            self.result_cache.update(result)
            self.queue_sz -= 1

    # prevent new tasks from being submitted
    # but keep existing tasks running
    # should be idempotent
    def close(self):
        if not self._closed:
            # send quit signal to child
            self.parent_conn.send([None, True])

            # keep track of closure,
            # so subsequent task submissions are rejected
            self._closed = True

    # after closing
    # wait for existing tasks to finish
    # should be idempotent
    def join(self):
        assert self._closed, "Must close before joining"

        try:
            self.proc.join()
        except ValueError as e:
            # .join() has probably been called multiple times
            # so the process has already been closed
            pass
        finally:
            self.proc.close()

        self.flush()
        self.parent_conn.close()

    # terminate child processes without waiting for them to finish
    # should be idempotent
    def terminate(self):
        try:
            a = self.proc.is_alive()
        except ValueError:
            # already closed
            # .is_alive seems to raise ValueError not return False if dead
            pass
        else:
            if a:
                try:
                    self.proc.close()
                except ValueError:
                    self.proc.terminate()
        self.parent_conn.close()
        self.child_conn.close()
        self._closed |= True

    def __del__(self):
        self.terminate()

    def action(self, payload):
        raise NotImplementedError("Implement this method in the subclass")


class AsyncResult:
    def __init__(self, id: UUID, child: Worker):
        assert isinstance(id, UUID)
        self.id = id
        self.child = child
        self.result: Union[Tuple[Any, None], Tuple[None, Exception]] = None

    # assume the result is in the self.child.result_cache
    # move it into self.result
    def _load(self):
        self.result = self.child.result_cache[self.id]
        del self.child.result_cache[self.id]  # prevent memory leak

    # Return the result when it arrives.
    # If timeout is not None and the result does not arrive within timeout seconds
    # then multiprocessing.TimeoutError is raised.
    # If the remote call raised an exception then that exception will be reraised by get().
    # .get() must remember the result
    # and return it again multiple times
    # delete it from the Worker.result_cache to avoid memory leak
    def get(self, timeout=10):
        if self.result is not None:
            (response, ex) = self.result
            if ex:
                raise ex
            else:
                return response
        elif self.id in self.child.result_cache:
            self._load()
            return self.get(0)
        else:
            self.wait(timeout)
            if not self.ready():
                raise TimeoutError("result not ready")
            else:
                return self.get(0)

    # Wait until the result is available or until timeout seconds pass.
    def wait(self, timeout=10):
        start_t = time()
        if self.result is None:
            self.child.flush()
            # the result we want might not be the next result
            # it might be the 2nd or 3rd next
            while (self.id not in self.child.result_cache) and \
                    ((timeout is None) or (time() - timeout < start_t)):
                if timeout is None:
                    self.child.parent_conn.poll()
                else:
                    elapsed_so_far = time() - start_t
                    remaining = timeout - elapsed_so_far
                    self.child.parent_conn.poll(remaining)
                if self.child.parent_conn.poll(0):
                    self.child.flush()

    # Return whether the call has completed.
    def ready(self):
        self.child.flush()
        return self.result or (self.id in self.child.result_cache)

    # Return whether the call completed without raising an exception.
    # Will raise ValueError if the result is not ready.
    def successful(self):
        if self.result is None:
            if not self.ready():
                raise ValueError("Result is not ready")
            else:
                self._load()

        return self.result[1] is None


class Pool:
    def __init__(self, processes=None, worker=Worker):
        if processes is None:
            self.num_processes = max(os.cpu_count(), 2)
        else:
            if processes < 0:
                raise ValueError("processes must be a positive integer")
            self.num_processes = max(processes, 2)

        self._closed = False
        self.Worker = worker

    def __enter__(self):
        self._closed = False

        self.children = [self.Worker() for _ in range(self.num_processes)]

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.join()
        self.terminate()

    def __del__(self):
        self.terminate()

    # prevent new tasks from being submitted
    # but keep existing tasks running
    def close(self):
        if not self._closed:
            for c in self.children:
                c.close()
            self._closed |= True

    # wait for existing tasks to finish
    def join(self):
        assert self._closed, "Must close before joining"
        for c in self.children:
            c.join()

    # terminate child processes without waiting for them to finish
    def terminate(self):
        for c in self.children:
            c.terminate()
        self._closed |= True

    def apply(self, args=(), kwds=None):
        ret = self.apply_async(args, kwds)
        return ret.get()

    def apply_async(self, args=(), kwds=None) -> AsyncResult:

        if self._closed:
            raise ValueError("Pool already closed")
        if kwds is None:
            kwds = {}


        # choose the first idle process if there is one
        # if not, choose the process with the shortest queue
        for c in self.children:
            c.flush()
        min_q_sz = min(c.queue_sz for c in self.children)
        c = random.choice([c for c in self.children if c.queue_sz <= min_q_sz])
        return c.submit(args, kwds)

    def schedule(self, iterable) -> List:
        results = [self.apply_async(args) for args in zip(iterable)]
        return [r.get() for r in results]

# example usage

# with Pool(processes = 8, worker=ValidatorWorker) as p:
#     payload = p.schedule(payload)\

#
# class ValidatorWorker(Worker):
#
#     def action(self, x):
#
#         return x*x