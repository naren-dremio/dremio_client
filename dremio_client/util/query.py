import time
import sys
from concurrent.futures.thread import ThreadPoolExecutor

from ..model.endpoints import job_results, job_status, sql
from six import reraise as _raise

executor = ThreadPoolExecutor(max_workers=8)

_job_states = {'NOT_SUBMITTED', 'STARTING', 'RUNNING', 'COMPLETED', 'CANCELED', 'FAILED', 'CANCELLATION_REQUESTED',
               'ENQUEUED'}
_done_job_states = {'COMPLETED', 'CANCELED', 'FAILED'}


def run(token, base_url, query):
    job = sql(token, base_url, query)
    job_id = job['id']
    while True:
        state = job_status(token, base_url, job_id)
        if state['jobState'] == 'COMPLETED':
            row_count = state.get('rowCount', 0)
            break
        if state['jobState'] in {'CANCELED', 'FAILED'}:
            raise ValueError("job failed " + state)
        time.sleep(10)
    count = 0
    while count < row_count:
        result = job_results(token, base_url, job_id, count)
        count += 100
        yield result


def run_async(token, base_url, query):
    return executor.submit(run, token, base_url, query)


def refresh_metadata(token, base_url, table):
    res = []
    for x in run(token, base_url, "ALTER PDS {} REFRESH METADATA FORCE UPDATE".format(table)):
        res.append(x)
    return res
