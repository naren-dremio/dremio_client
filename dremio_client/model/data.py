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
from recordclass import recordclass
import simplejson as json

from .endpoints import catalog_item, collaboration_tags, collaboration_wiki, refresh_pds, delete_catalog, \
    update_catalog, set_catalog
from ..util import refresh_metadata
from ..error import DremioException


def namedtuple(name, args):
    """
    shim to make recordclass look like namedtuple to constructors

    :param name: name of record class
    :param args: fields in record class
    :return: recordclass class object
    """
    return recordclass(name, ' '.join(args))


class VoteMetadata(namedtuple('VoteMetadata', [
    "id",
    "votes",
    "datasetId",  # todo link to dataset
    "datasetPath",  # todo link to dataset
    "datasetType",
    "datasetReflectionCount",
    "entityType"
])):
    def to_json(self):
        return json.dumps(self._asdict())


class QueueMetadata(namedtuple('QueueMetadata', [
    "id",
    "tag",
    "name",
    "cpuTier",
    "maxAllowedRunningJobs",
    "maxStartTimeoutMs"
])):
    def to_json(self):
        return json.dumps(self._asdict())


class RuleMetadata(namedtuple('RuleMetadata', [
    "name",
    "conditions",
    "acceptId",
    "acceptName",
    "action",
    "id"
])):
    def to_json(self):
        return json.dumps(self._asdict())


class ReflectionSummaryMetadata(namedtuple('ReflectionSummaryMetadata', ["entityType",
                                                                         "id",
                                                                         "createdAt",
                                                                         "updatedAt",
                                                                         "type",
                                                                         "name",
                                                                         "datasetId",
                                                                         "datasetPath",
                                                                         "datasetType",
                                                                         "currentSizeBytes",
                                                                         "totalSizeBytes",
                                                                         "enabled",
                                                                         "status",
                                                                         ])):
    def to_json(self):
        return json.dumps(self._asdict())


class ReflectionMetadata(namedtuple('ReflectionMetadata', ["entityType",
                                                           "id",
                                                           "tag",
                                                           "name",
                                                           "enabled",
                                                           "createdAt",
                                                           "updatedAt",
                                                           "type",
                                                           "datasetId",
                                                           "currentSizeBytes",
                                                           "totalSizeBytes",
                                                           "status",
                                                           "dimensionFields",
                                                           "measureFields",
                                                           "displayFields",
                                                           "distributionFields",
                                                           "partitionFields",
                                                           "sortFields",
                                                           "partitionDistributionStrategy"
                                                           ])):
    def to_json(self):
        return json.dumps(self._asdict())


RootMetaData = namedtuple('Root', ['id'])
WikiData = namedtuple('WikiData', ['text', 'version'])
TagsData = namedtuple('TagsData', ['tags', 'version'])
MetadataPolicy = namedtuple('MetadataPolicy', ['authTTLMs',
                                               'datasetRefreshAfterMs',
                                               'datasetExpireAfterMs',
                                               'namesRefreshMs',
                                               'datasetUpdateMode'])
AccessControl = namedtuple('AccessControl', ['id', 'permission'])
AccessControlList = namedtuple('AccessControlList', ['users', 'groups', 'version'])
SourceState = namedtuple('SourceState', ['status', 'message'])

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
                                                 'approximateStatisticsAllowed',
                                                 'accessControlList'])
SpaceMetaData = namedtuple('SpaceMetaData', ['entityType', 'id', 'name', 'tag', 'path', 'accessControlList'])
FolderMetaData = namedtuple('FolderMetaData', ['entityType', 'id', 'path', 'tag', 'accessControlList'])
FileMetaData = namedtuple('FileMetaData', ['entityType', 'id', 'path', 'accessControlList'])
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
                                               'path',
                                               'accessControlList'
                                               ])


def _clean(string):
    return string.replace('"', '').replace(' ', '_').replace(
        '-', '_').replace('@', '').replace('.', '_')


def get_path(item, trim_path):
    path = item.get('path', [item.get('name', None)])
    return path[trim_path:] if trim_path > 0 else path


def create(item, token, base_url, flight_endpoint, trim_path=0, ssl_verify=True, dirty=False):
    path = get_path(item, trim_path)
    name = _clean('_'.join(path))
    obj_type = item.get('type', None)
    container_type = item.get('containerType', None)
    entity_type = item.get('entityType', None)
    sql = item.get('sql', None)
    if obj_type == 'CONTAINER':
        if container_type == 'HOME':
            return name, Home(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == 'SPACE':
            return name, Space(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == 'SOURCE':
            return name, Source(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif container_type == 'FOLDER':
            return name, Folder(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
    else:
        if obj_type == 'DATASET':
            if sql:
                return name, VirtualDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
            else:
                return name, PhysicalDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif obj_type == 'FILE':
            return name, File(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        if entity_type == 'source':
            return name, Source(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == 'folder':
            return name, Folder(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == 'home':
            return name, Home(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == 'space':
            return name, Space(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
        elif entity_type == 'dataset':
            if 'VIRTUAL' in obj_type:
                return name, VirtualDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
            else:
                return name, PhysicalDataset(token, base_url, flight_endpoint, ssl_verify, dirty, **item)
    raise KeyError("unsupported type")


class Catalog(dict):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False):
        dict.__init__(self)
        self._base_url = base_url
        self._token = token
        self._flight_endpoint = flight_endpoint
        self._ssl_verify = ssl_verify
        self._dirty = dirty
        self.meta = None

        def try_id_and_path(x, y):
            try:
                return catalog_item(token, base_url, x, ssl_verify=ssl_verify)
            except Exception:  # NOQA
                return catalog_item(token, base_url, path=y, ssl_verify=ssl_verify)

        self._catalog_item = try_id_and_path

    def keys(self):
        keys = dict.keys(self)
        return [i for i in keys if i not in {
            '_catalog_item', '_base_url', '_token', '_flight_endpoint'}]

    def commit(self):
        if self._dirty:
            if self.meta.id:
                self.meta = _put(self)
            else:
                self.meta = _post(self)
            self._dirty = False

    def get(self):
        dir(self)
        return self

    def __dir__(self):
        if len(self.keys()) == 0 and 'meta' in self.__dict__:
            if self.meta.entityType in {'source', 'home', 'space', 'folder', 'root', 'dataset'}:
                result = self._catalog_item(self.meta.id if hasattr(self.meta, 'id') else None,
                                            self.meta.path if hasattr(self.meta, 'path') else None)
                _, obj = create(result, self._token,
                                self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
                self.update(obj)
                self.meta = self.meta._replace(**{k: v for k, v in obj.meta._asdict().items() if v})
                return list(self.keys())
        return list(self.keys()) + ['_repr_html_']

    def to_json(self):
        result = self.meta._asdict()
        children = list()
        for child in self.keys():
            try:
                children.append(child.to_json())
            except:  # NOQA
                pass
        if len(children) > 1:
            result['children'] = children
        return json.dumps(result)

    def __getattr__(self, item):
        if item == '_ipython_canary_method_should_not_exist_':
            raise AttributeError
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

    def wiki(self):
        result = collaboration_wiki(self._token, self._base_url, self.meta.id, ssl_verify=self._ssl_verify)
        return make_wiki(result)

    def tags(self):
        result = collaboration_tags(self._token, self._base_url, self.meta.id, ssl_verify=self._ssl_verify)
        return make_tags(result)

    def __repr__(self):
        return self.to_json()

    def _repr_html_(self):
        try:
            tags = self.tags().tags
            tags_html = '\n'.join(['<span class="badge">{}</span>'.format(i) for i in tags])
            tag_html = '<div> <strong>Tags: </strong> {} </div>'.format(tags_html)
        except DremioException:
            tag_html = ''
        try:
            import markdown
            wiki = self.wiki()
            text = wiki.text
            html = markdown.markdown(text)
            return tag_html + html
        except ImportError:
            return self.__repr__()
        except DremioException:
            return self.__repr__()

    def __delitem__(self, key):
        return self.remove()

    def remove(self):
        return delete_catalog(self._token, self._base_url, self.meta.id, self.meta.tag, self._ssl_verify)


def _put(self):
    cid = self.meta.id
    result = update_catalog(self._token, self._base_url, cid, self.meta._asdict(), self._ssl_verify)
    name, obj = create(result, self._token,
                       self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
    return obj.meta


def _post(self):
    json = self.meta._asdict()
    for i in ('state', 'id', 'tag', 'createdAt'):
        if i in json:
            del json[i]
    result = set_catalog(self._token, self._base_url, json, self._ssl_verify)
    name, obj = create(result, self._token,
                       self._base_url, self._flight_endpoint, ssl_verify=self._ssl_verify)
    return obj.meta


class Root(Catalog):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, ssl_verify=True, dirty=False):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = RootMetaData('root')

    def add(self, item):
        name, obj = create(item, self._token, self._base_url,
                           self._flight_endpoint, ssl_verify=self._ssl_verify)
        self[name] = obj

    def add_by_path(self, item, new_entity=True):
        if new_entity:
            id = item.pop('id')
            tag = item.pop('tag')
        name, obj = create(item, self._token, self._base_url,
                           self._flight_endpoint, ssl_verify=self._ssl_verify, dirty=True)
        if new_entity:
            item['id'] = id
            item['tag'] = tag
        base = self
        subpath = list()
        for p in obj.meta.path[:-1]:
            subpath.append(_clean(p))
            try:
                base = base[_clean(p)]
            except KeyError as e:
                if isinstance(obj, PhysicalDataset):
                    self.add({'entityType': 'folder', 'path': subpath})
                else:
                    raise e
        base[_clean(obj.meta.path[-1])] = obj


def _get_acl(acl):
    if not acl:
        return
    return [AccessControl(ac.get('id'), ac.get('permission')) for ac in acl]


def _get_acls(acl):
    if not acl:
        return
    return AccessControlList(
        users=_get_acl(acl.get('users')),
        groups=_get_acl(acl.get('groups')),
        version=acl.get('version')
    )


class Space(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = SpaceMetaData(
            entityType='space',
            id=kwargs.get('id'),
            tag=kwargs.get('tag'),
            name=kwargs.get('name'),
            path=kwargs.get('path'),
            accessControlList=_get_acls(kwargs.get('accessControlList'))
        )
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint,
                                trim_path=len(kwargs.get('path', list())), ssl_verify=self._ssl_verify)
            self[name] = item


class Home(Space):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Space.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)
        self.meta = self.meta._replace(entityType='home')


class Folder(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = FolderMetaData(
            entityType='folder',
            id=kwargs.get('id', None),
            tag=kwargs.get('tag', None),
            path=kwargs.get('path', None),
            accessControlList=_get_acls(kwargs.get('accessControlList'))
        )
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint,
                                trim_path=len(kwargs.get('path', list())), ssl_verify=self._ssl_verify)
            self[name] = item


class File(Catalog):

    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = FileMetaData(
            entityType='file',
            id=kwargs.get('id', None),
            path=kwargs.get('path', None),
            accessControlList=_get_acls(kwargs.get('accessControlList'))
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
        path=kwargs.get('path'),
        accessControlList=_get_acls(kwargs.get('accessControlList'))
    )


class Source(Catalog):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
        self.meta = _get_source_meta(kwargs)
        path = self.meta.path
        for child in kwargs.get('children', list()):
            name, item = create(
                child, token, base_url, self._flight_endpoint, trim_path=(
                    len(path) if path else 1), ssl_verify=self._ssl_verify)
            self[name] = item


class Dataset(Catalog):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Catalog.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty)
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
            approximateStatisticsAllowed=kwargs.get('approximateStatisticsAllowed'),
            accessControlList=_get_acls(kwargs.get('accessControlList'))
        )

    def query(self):
        return self.sql("select * from {}")

    def sql(self, sql):
        return self._flight_endpoint(sql)


class PhysicalDataset(Dataset):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)

    def metadata_refresh(self):
        refresh_metadata(self._token, self._base_url,
                         ".".join(self.meta.path))

    def refresh(self):
        refresh_pds(self._token, self._base_url, self.meta.id, self._ssl_verify)


class VirtualDataset(Dataset):
    def __init__(self, token=None, base_url=None,
                 flight_endpoint=None, ssl_verify=True, dirty=False, **kwargs):
        Dataset.__init__(self, token, base_url, flight_endpoint, ssl_verify, dirty, **kwargs)


def make_reflection(data, summary=False):
    if summary:
        return ReflectionSummaryMetadata(
            entityType="reflection-summary",
            id=data.get("id"),
            createdAt=data.get("createdAt"),
            updatedAt=data.get("updatedAt"),
            type=data.get("type"),
            name=data.get("name"),
            datasetId=data.get("datasetId"),
            datasetPath=data.get("datasetPath"),
            datasetType=data.get("datasetType"),
            currentSizeBytes=data.get("currentSizeBytes"),
            totalSizeBytes=data.get("totalSizeBytes"),
            enabled=data.get("enabled"),
            status=data.get("status"),
        )
    return ReflectionMetadata(
        entityType="reflection",
        id=data.get("id"),
        tag=data.get("tag"),
        name=data.get("name"),
        enabled=data.get("enabled"),
        createdAt=data.get("createdAt"),
        updatedAt=data.get("updatedAt"),
        type=data.get("type"),
        datasetId=data.get("datasetId"),  # todo link to dataset
        currentSizeBytes=data.get("currentSizeBytes"),
        totalSizeBytes=data.get("totalSizeBytes"),
        status=data.get("status"),  # todo object
        dimensionFields=data.get("dimensionFields"),  # todo object
        measureFields=data.get("measureFields"),  # todo object
        displayFields=data.get("displayFields"),  # todo object
        distributionFields=data.get("distributionFields"),  # todo object
        partitionFields=data.get("partitionFields"),  # todo object
        sortFields=data.get("sortFields"),  # todo object
        partitionDistributionStrategy=data.get("partitionDistributionStrategy"),
    )


def make_tags(tags):
    return TagsData(tags=tags.get('tags'), version=tags.get('version'))


def make_wiki(wiki):
    return WikiData(text=wiki.get('text'), version=wiki.get('version'))


def make_wlm_rule(rule):
    return RuleMetadata(
        id=rule.get("id"),
        conditions=rule.get("conditions"),
        name=rule.get("name"),
        acceptId=rule.get("acceptId"),
        acceptName=rule.get("acceptName"),
        action=rule.get("action")
    )


def make_wlm_queue(queue):
    return QueueMetadata(
        id=queue.get("id"),
        tag=queue.get("tag"),
        name=queue.get("name"),
        cpuTier=queue.get("cpuTier"),
        maxAllowedRunningJobs=queue.get("maxAllowedRunningJobs"),
        maxStartTimeoutMs=queue.get("maxStartTimeoutMs")
    )


def make_vote(vote):
    return VoteMetadata(
        id=vote.get("id"),
        votes=vote.get("votes"),
        datasetId=vote.get("datasetId"),
        datasetPath=vote.get("datasetPath"),
        datasetType=vote.get("datasetType"),
        datasetReflectionCount=vote.get("datasetReflectionCount"),
        entityType="vote-summary"
    )
