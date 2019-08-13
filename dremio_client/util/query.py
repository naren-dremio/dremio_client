#
# Copyright (c) 2019 Ryan Murray.
#
# This file is part of Dremio Client
# (see https://github.com/rymurr/dremio_client).
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import time
from concurrent.futures.thread import ThreadPoolExecutor

from ..model.endpoints import job_results, job_status, sql

executor = ThreadPoolExecutor(max_workers=8)

_job_states = {
    'NOT_SUBMITTED',
    'STARTING',
    'RUNNING',
    'COMPLETED',
    'CANCELED',
    'FAILED',
    'CANCELLATION_REQUESTED',
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
            raise ValueError("job failed " + str(state))
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
    for x in run(token, base_url,
                 "ALTER PDS {} REFRESH METADATA FORCE UPDATE".format(table)):
        res.append(x)
    return res
