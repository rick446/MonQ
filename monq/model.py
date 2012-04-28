import time
import logging
import traceback
from datetime import datetime

import pymongo

from ming import collection, Session, Field, Index
from ming import schema as S
from ming.utils import LazyProperty
from ming.odm import ThreadLocalODMSession, session

log = logging.getLogger(__name__)

doc_session = Session.by_name('monq')
odm_session = ThreadLocalODMSession(doc_session)

STATES=('ready', 'busy', 'error', 'complete')
RESULT_TYPES=('keep', 'forget')

def task(func):
    '''Decorator to add some methods to task functions'''
    def post(*args, **kwargs):
        return TaskObject.post(func, args, kwargs)
    func.post = post
    return func

TaskDoc = collection(
    'monq.task', doc_session,
    Field('_id', S.ObjectId),
    Field('state', S.OneOf(*STATES)),
    Field('priority', int),
    Field('result_type', S.OneOf(*RESULT_TYPES)),
    Field('time', dict(
            queue=S.DateTime(if_missing=datetime.utcnow),
            start=S.DateTime(if_missing=None),
            stop=S.DateTime(if_missing=None))),
    Field('task', dict(
            name=str,
            args=[None],
            kwargs={str:None})),
    Field('result', None, if_missing=None),
    Field('process', str),
    Index(
        ('state', 1),
        ('priority', -1),
        ('time.queue', 1)),
    Index('state', 'time_queue'))

class TaskObject(object):

    @LazyProperty
    def function(self):
        '''The function that is called by this task'''
        smod, sfunc = str(self.task.name).rsplit('.', 1)
        cur = __import__(smod, fromlist=[sfunc])
        return getattr(cur, sfunc)

    @classmethod
    def post(cls,
             function,
             args=None,
             kwargs=None,
             result_type='forget',
             priority=10):
        '''Create a new task object.'''
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        task_name = '%s.%s' % (
            function.__module__,
            function.__name__)
        obj = cls(
            state='ready',
            priority=priority,
            result_type=result_type,
            task=dict(
                name=task_name,
                args=args,
                kwargs=kwargs),
            result=None)
        session(obj).flush(obj)
        return obj

    @classmethod
    def get(cls, process='worker', state='ready', waitfunc=None):
        '''Get the highest-priority, oldest, ready task and lock it to the
        current process.
        '''
        sort = [ ('priority', -1), ('time_queue', 1) ]
        while True:
            try:
                obj = cls.query.find_and_modify(
                    query=dict(state=state),
                    update={
                        '$set': dict(
                            state='busy',
                            process=process)
                        },
                    new=True,
                    sort=sort)
                if obj is not None: return obj
            except pymongo.errors.OperationFailure, exc:
                if 'No matching object found' not in exc.args[0]:
                    raise
            if waitfunc is None: return None
            waitfunc()

    @classmethod
    def timeout_tasks(cls, older_than):
        '''Mark all busy tasks older than a certain datetime as 'ready' again.
        Used to retry 'stuck' tasks.'''
        spec = dict(state='busy')
        spec['time.start'] = {'$lt':older_than}
        cls.query.update(spec, {'$set': dict(state='ready')}, multi=True)

    @classmethod
    def clear_complete(cls):
        '''Delete the task objects for complete tasks'''
        cls.query.remove(dict(state='complete'))

    @classmethod
    def run_ready(cls, worker=None):
        '''Run all the tasks that are currently ready, returning the number of
        tasks thus run'''
        i=0
        for i, task in enumerate(cls.query.find(dict(state='ready')).all()):
            task.process = worker
            task()
        return i

    def __call__(self):
        '''Call the task function with its arguments.'''
        self.time_start = datetime.utcnow()
        session(self).flush(self)
        log.info('%r', self)
        try:
            func = self.function
            self.result = func(*self.args, **self.kwargs)
            self.state = 'complete'
            if self.result_type == 'forget':
                self.query.delete()
            return self.result
        except Exception, exc:
            log.exception('%r', self)
            self.state = 'error'
            if hasattr(exc, 'format_error'):
                self.result = exc.format_error()
                log.error(self.result)
            else:
                self.result = traceback.format_exc()
            raise
        finally:
            self.time_stop = datetime.utcnow()
            session(self).flush(self)

    def join(self, poll_interval=0.1):
        '''Wait until this task is either complete or errors out,
        then return the result.'''
        while self.state not in ('complete', 'error'):
            time.sleep(poll_interval)
            self.query.find(dict(_id=self._id), refresh=True).first()
        return self.result

    @classmethod
    def list(cls, state='ready'):
        '''Print all tasks of a certain status to sys.stdout.  Used for
        debugging.'''
        q = cls.query.find(dict(state=state))
        lines = map(repr, q)
        return '\n'.join(lines)

odm_session.mapper(TaskObject, TaskDoc)




