import threading
from unittest import TestCase
from datetime import datetime, timedelta

from ming import Session, datastore

from monq.model import TaskDoc, TaskObject, task

@task
def test_task(*args, **kwargs):
    return 42

@task
def bad_task(*args, **kwargs):
    raise ValueError

class TestModel(TestCase):

    def setUp(self):
        self.ds = datastore.DataStore(
            'mim:///', database='test')
        self.session = Session.by_name('monq')
        self.session.bind = self.ds
        self.ds.db.clear()

    def test_post_task(self):
        test_task.post(1, 2, a=5)
        obj = TaskDoc.m.get()
        # pop properties we don't care about
        obj.pop('_id')
        obj.pop('time')
        self.assertEqual(obj, dict(
                state='ready',
                priority=10,
                result_type='forget',
                task=dict(
                    name='monq.tests.test_task',
                    args=[1,2],
                    kwargs=dict(a=5)),
                result=None,
                process=None))

    def test_task_function(self):
        test_task.post(1, 2, a=5)
        obj = TaskObject.query.get()
        self.assertEqual(obj.function, test_task)

    def test_get_task(self):
        test_task.post(1, 2, a=5)
        obj = TaskObject.get()
        self.assertEqual(obj.process, 'worker')
        self.assertEqual(obj.state, 'busy')

    def test_get_empty_queue(self):
        obj = TaskObject.get()
        self.assertEqual(obj, None)

    def test_get_empty_queue_with_wait(self):
        def waitfunc():
            raise ValueError
        self.assertRaises(
            ValueError, TaskObject.get, waitfunc=waitfunc)

    def test_timeout_tasks(self):
        # Create busy task
        doc = TaskDoc.make(dict(
                task=dict(name='foo',args=[],kwargs={}),
                time=dict(start=datetime.utcnow()),
                state='busy'))
        doc.m.save()
        # timeout tasks
        TaskObject.timeout_tasks(
            datetime.utcnow() + timedelta(seconds=10))
        doc = TaskDoc.m.get()
        # Make sure the doc is ready and unlocked
        self.assertEqual(doc.state, 'ready')
        self.assertEqual(doc.process, None)

    def test_clear_complete_forget(self):
        # Create complete, 'forget' task
        doc = TaskDoc.make(dict(
                task=dict(name='foo', args=[], kwargs={}),
                result_type='forget',
                state='complete'))
        doc.m.save()
        # timeout tasks
        TaskObject.clear_complete()
        # Make sure the doc is gone
        self.assertEqual(TaskDoc.m.get(), None)
        
    def test_clear_complete_keep(self):
        # Create complete, 'keep' task
        doc = TaskDoc.make(dict(
                task=dict(name='foo', args=[], kwargs={}),
                result_type='keep',
                state='complete'))
        doc.m.save()
        # timeout tasks
        TaskObject.clear_complete()
        # Make sure the doc is gone
        self.assertEqual(TaskDoc.m.find().count(), 1)
        
    def test_run_ready(self):
        # Create two tasks
        test_task.post()
        test_task.post()
        # Run all the ready tasks
        TaskObject.run_ready()
        # Verify that all the tasks are complete
        self.assertEqual(
            0, TaskDoc.m.find(dict(state='ready')).count())
        self.assertEqual(
            2, TaskDoc.m.find(dict(state='complete')).count())
        
    def test_run_task(self):
        test_task.post()
        t = TaskObject.get()
        self.assertEqual(t(), 42)
        self.assertEqual(t.state, 'complete')
        self.assertEqual(t.result, 42)

    def test_task_with_exceptions(self):
        bad_task.post()
        t = TaskObject.get()
        self.assertRaises(ValueError, t)
        self.assertEqual(t.state, 'error')
        self.assertEqual(
            'ValueError', t.result.split('\n')[-2])

    def test_join_ok(self):
        task = test_task.post()
        thread = threading.Thread(
            target=lambda:task.join())
        thread.daemon = True
        thread.start()
        task()
        thread.join(1)
        self.assertEqual(thread.is_alive(), False)

    def test_join_timeout(self):
        task = test_task.post()
        thread = threading.Thread(
            target=lambda:task.join())
        thread.daemon = True
        thread.start()
        # Do *not* run the task
        thread.join(0.01)
        self.assertEqual(thread.is_alive(), True)

    def test_list_tasks(self):
        tasks = [ test_task.post() ]
        self.assertEqual(
            TaskObject.list(),
            '\n'.join(map(repr, tasks)))
        tasks.append(test_task.post())
        self.assertEqual(
            len(TaskObject.list()),
            len('\n'.join(map(repr, tasks))))
        
        
        
        
