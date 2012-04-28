from unittest import TestCase
from ming import Session, datastore

import mock

from monq.model import TaskDoc, TaskObject, task

@task
def test_task(*args, **kwargs):
    return mock.Mock()

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
        

