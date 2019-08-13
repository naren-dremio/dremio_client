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
from collections import namedtuple
import simplejson as json

from .endpoints import catalog_item
from ..util import refresh_metadata


DatasetMetaData = namedtuple('DatasetMetaData', ['entityType',
                                                 'id',
                                                 'path',
                                                 'tag',
                                                 'type',
                                                 'fields',
                                                 'createdAt',
                                                 'accelerationRefreshPolicy',
                                                 'sql',
                                                 'sqlContext',
                                                 'format',
                                                 'approximateStatisticsAllowed'])
SpaceMetaData = namedtuple('SpaceMetaData', ['entityType', 'id', 'name', 'tag', 'path'])
FolderMetaData = namedtuple('FolderMetaData', ['entityType', 'id', 'path', 'tag'])
FileMetaData = namedtuple('FileMetaData', ['entityType', 'id', 'path'])
SourceState = namedtuple('SourceState', ['status', 'message'])
MetadataPolicy = namedtuple('MetadataPolicy', ['authTTLMs',
                                               'datasetRefreshAfterMs',
                                               'datasetExpireAfterMs',
                                               'namesRefreshMs',
                                               'datasetUpdateMode'])
SourceMetadata = namedtuple('SourceMetadata', ['entityType',
                                               'id',
                                               'name',
                                               'description',
                                               'tag',
                                               'type',
                                               'config',
                                               'createdAt',
                                               'metadataPolicy',
                                               'state',
                                               'accelerationGracePeriodMs',
                                               'accelerationRefreshPeriodMs',
                                               'accelerationNeverExpire',
                                               'accelerationNeverRefresh',
                                               'path'
                                               ])


def _clean(string):
    return string.replace('"', '').replace(' ', '_').replace(
        '-', '_').replace('@', '').replace('.', '_')


def get_path(item, trim_path):
    path = item.get('path', [item.get('name', None)])
    return path[trim_path:] if trim_path > 0 else path


def create(item, token, base_url, flight_endpoint, trim_path=0):
    path = get_path(item, trim_path)
    name = _clean('_'.join(path))
    obj_type = item.get('type', None)
    container_type = item.get('containerType', None)
    entity_type = item.get('entityType', None)
    dataset_type = item.get('datasetType', None)
    if obj_type == 'CONTAINER':
        if container_type == 'HOME':
            return name, Home(token, base_url, flight_endpoint, **item)
        elif container_type == 'SPACE':
            return name, Space(token, base_url, flight_endpoint, **item)
        elif container_type == 'SOURCE':
            return name, Source(token, base_url, flight_endpoint, **item)
        elif container_type == 'FOLDER':
            return name, Folder(token, base_url, flight_endpoint, **item)
    else:
        if obj_type == 'DATASET':
            if dataset_type == 'PROMOTED':
                return name, PhysicalDataset(token, base_url, flight_endpoint, **item)
            else:
                return name, VirtualDataset(token, base_url, flight_endpoint, **item)
        elif obj_type == 'FILE':
            return name, File(token, base_url, flight_endpoint, **item)
        if entity_type == 'source':
            return name, Source(token, base_url, flight_endpoint, **item)
        elif entity_type == 'folder':
            return name, Folder(token, base_url, flight_endpoint, **item)
        elif entity_type == 'home':
            return name, Home(token, base_url, flight_endpoint, **item)
        elif entity_type == 'space':
            return name, Space(token, base_url, flight_endpoint, **item)
    raise KeyError("unsupported type")


class Catalog(dict):

    def __init__(self, token=None, base_url=None, flight_endpoint=None):
        dict.__init__(self)
        self._base_url = base_url
        self._token = token
        self._flight_endpoint = flight_endpoint

        def try_id_and_path(x, y):
            try:
                return catalog_item(token, base_url, x)
            except Exception as e:
                return catalog_item(token, base_url, path=y)

        self._catalog_item = try_id_and_path

    def keys(self):
        keys = dict.keys(self)
        return [i for i in keys if i not in {
            '_catalog_item', '_base_url', '_token', '_flight_endpoint'}]

    def commit(self):
        s = self.to_json()
        # todo do put here!

    def get(self):
        dir(self)
        return self

    def __dir__(self):
        if len(self.keys()) == 0 and 'meta' in self.__dict__:
            if self.meta.entityType in {'source', 'home', 'space', 'folder', 'root'}:
                result = self._catalog_item(self.meta.id if hasattr(self.meta, 'id') else None,
                                            self.meta.path if hasattr(self.meta, 'path') else None)
                name, obj = create(result, self._token,
                                   self._base_url, self._flight_endpoint)
                self.update(obj)
                self.meta = self.meta._replace(**{k: v for k, v in obj.meta._asdict().items() if v})
                return list(self.keys())
        return list(self.keys())

    def to_json(self):
        result = self.meta._asdict()
        children = list()
        for child in self.keys():
            try:
                children.append(child.to_json())
            except:
                pass
        if len(children) > 1:
            result['children'] = children
        return json.dumps(result)

    def __getattr__(self, item):
        try:
            value = dict.__getitem__(self, item)
            if value is None:
                raise KeyError()
            if isinstance(value, Catalog) and value['_base_url'] is None:
                raise KeyError()
            return value
        except KeyError:
            self.__dir__()
            return dict.__getitem__(self, item)


class Root(Catalog):

    def __init__(self, token=None, base_url=None, flight_endpoint=None):
        Catalog.__init__(self, token, base_url, flight_endpoint)

    def add(self, item):
        name, obj = create(item, self._token, self._base_url,
                           self._flight_endpoint)
        self[name] = obj


class Space(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint)
        self.meta = SpaceMetaData(
            entityType='space',
            id=kwargs.get('id'),
            tag=kwargs.get('tag'),
            name=kwargs.get('name'),
            path=kwargs.get('path')
        )
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint,
                                trim_path=len(kwargs.get('path', list())))
            self[name] = item


class Home(Space):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Space.__init__(self, token, base_url, flight_endpoint, **kwargs)
        self.meta = self.meta._replace(entityType='home')


class Folder(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint)
        self.meta = FolderMetaData(
            entityType='folder',
            id=kwargs.get('id', None),
            tag=kwargs.get('tag', None),
            path=kwargs.get('path', None)
        )
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint,
                                trim_path=len(kwargs.get('path', list())))
            self[name] = item


class File(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint)
        self.meta = FileMetaData(
            entityType='file',
            id=kwargs.get('id', None),
            path=kwargs.get('path', None)
        )


def _get_source_type(source_type):
    return source_type  # todo may do more with this at some point


def _get_source_config(config):
    return config  # todo these should be turned into source specific objects. Will do at some stage


def _get_metadata_policy(metadata_policy):
    if not metadata_policy:
        return None
    return MetadataPolicy(
        authTTLMs=metadata_policy.get('authTTLMs'),
        datasetRefreshAfterMs=metadata_policy.get('datasetRefreshAfterMs'),
        datasetExpireAfterMs=metadata_policy.get('datasetExpireAfterMs'),
        namesRefreshMs=metadata_policy.get('namesRefreshMs'),
        datasetUpdateMode=metadata_policy.get('datasetUpdateMode')
    )


def _get_source_state(state):
    if not state:
        return None
    return SourceState(
        status=state.get('status'),
        message=state.get('message')
    )


def _get_source_meta(kwargs):
    return SourceMetadata(
        entityType="source",
        id=kwargs.get('id'),
        name=kwargs.get('name'),
        description=kwargs.get('description'),
        tag=kwargs.get('tag'),
        type=_get_source_type(kwargs.get('type')),
        config=_get_source_config(kwargs.get('config')),
        createdAt=kwargs.get('createdAt'),
        metadataPolicy=_get_metadata_policy(kwargs.get('metadataPolicy')),
        state=_get_source_state(kwargs.get('state')),
        accelerationGracePeriodMs=kwargs.get('accelerationGracePeriodMs'),
        accelerationRefreshPeriodMs=kwargs.get('accelerationRefreshPeriodMs'),
        accelerationNeverExpire=kwargs.get('accelerationNeverExpire'),
        accelerationNeverRefresh=kwargs.get('accelerationNeverRefresh'),
        path=kwargs.get('path')
    )


class Source(Catalog):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint)
        self.meta = _get_source_meta(kwargs)
        path = self.meta.path
        for child in kwargs.get('children', list()):
            name, item = create(
                child, token, base_url, self._flight_endpoint, trim_path=(
                    len(path) if path else 1))
            self[name] = item


class Dataset(Catalog):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint)
        self.meta = DatasetMetaData(
            entityType='dataset',
            id=kwargs.get('id'),
            path=kwargs.get('path'),
            tag=kwargs.get('tag'),
            type=kwargs.get('type'),
            fields=kwargs.get('fields'),
            createdAt=kwargs.get('path'),
            accelerationRefreshPolicy=kwargs.get('path'),
            sql=kwargs.get('sql'),
            sqlContext=kwargs.get('sqlContext'),
            format=kwargs.get('format'),
            approximateStatisticsAllowed=kwargs.get('approximateStatisticsAllowed')
        )

    def query(self):
        return self.sql("select * from {}")

    def sql(self, sql):
        return self._flight_endpoint(sql)

    def metadata_refresh(self):
        refresh_metadata(self._token, self._base_url,
                         ".".join(self.meta.path))


class PhysicalDataset(Dataset):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, **kwargs)


class VirtualDataset(Dataset):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, **kwargs)
