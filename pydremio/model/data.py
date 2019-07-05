from .endpoints import catalog_item
from addict import Dict


def _clean(string):
    return string.replace('"', '').replace(' ', '_').replace('-', '_').replace('@', '').replace('.', '_')


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
        elif obj_type == 'FILE':
            return name, File(token, base_url, flight_endpoint, **item)
        if entity_type == 'source':
            return name, Source(token, base_url, flight_endpoint, **item)
        elif entity_type == 'folder':
            return name, Folder(token, base_url, flight_endpoint, **item)
    raise KeyError("unsupported type")


class Catalog(Dict):

    def __init__(self, entity_type, token=None, base_url=None, flight_endpoint=None, id=None, tag=None):
        Dict.__init__(self)
        self._base_url = base_url
        self._token = token
        self._flight_endpoint = flight_endpoint
        self['meta'] = {'entity_type': entity_type,
                        'id': id,
                        'tag': tag}

        def try_id_and_path(x, y):
            try:
                return catalog_item(token, base_url, x)
            except Exception:
                return catalog_item(token, base_url, path=y)

        self._catalog_item = try_id_and_path

    def keys(self):
        keys = Dict.keys(self)
        return [i for i in keys if i not in {'_catalog_item', '_base_url', '_token', '_flight_endpoint'}]

    def __dir__(self):
        if len(self.keys()) == 1 and 'meta' in self.keys():
            if self.meta['entity_type'] in {'source', 'home', 'space', 'folder', 'root'}:
                result = self._catalog_item(self.meta['id'], self.meta.get('path', None))
                name, obj = create(result, self._token, self._base_url, self._flight_endpoint)
                self.update(obj)
                return list(self.keys())
            # if self.meta.type == 'DATASET':
            #     self.query = lambda : query("select * from {}", None)
            #     self.sql = lambda s: query(s, None)
        return list(self.keys())

    def __getattr__(self, item):
        try:
            value = Dict.__getattr__(self, item)
            if value is None:
                raise KeyError()
            if isinstance(value, Catalog) and value['_base_url'] is None:
                raise KeyError()
            return value
        except KeyError:
            self.__dir__()
            return Dict.__getattr__(self, item)


class Root(Catalog):

    def __init__(self, token=None, base_url=None, flight_endpoint=None):
        Catalog.__init__(self, "root", token, base_url, flight_endpoint)

    def add(self, item):
        name, obj = create(item, self._token, self._base_url, self._flight_endpoint)
        self[name] = obj


class Space(Catalog):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Catalog.__init__(self, "space", token, base_url, flight_endpoint, kwargs.get('id', None),
                         kwargs.get('tag', None))
        self.meta['name'] = kwargs.get('name', None)
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint,
                                trim_path=len(kwargs.get('path', list())))
            self[name] = item


class Home(Space):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Space.__init__(self, token, base_url, flight_endpoint, **kwargs)
        self.meta['entity_type'] = "home"


class Folder(Space):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Space.__init__(self, token, base_url, flight_endpoint, **kwargs)
        self.meta['entity_type'] = "folder"
        self.meta['path'] = kwargs.get('path', None)


class File(Space):

    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Catalog.__init__(self, "file", token, base_url, flight_endpoint, kwargs.get('id', None), None)
        self.meta['path'] = kwargs.get('path', None)


class Source(Catalog):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Catalog.__init__(self, "source", token, base_url, flight_endpoint, kwargs.get('id', None),
                         kwargs.get('tag', None))
        for i in (
                'path', 'type', 'config', 'createdAt', 'metadataPolicy', 'state', 'type', 'containerType', 'tag', 'id',
                'accelerationGracePeriodMs', 'accelerationRefreshPeriodMs', 'accelerationNeverExpire',
                'accelerationNeverRefresh'):
            self.meta[i] = kwargs.get(i, None)
        path = self.meta.get('path', list())
        for child in kwargs.get('children', list()):
            name, item = create(child, token, base_url, self._flight_endpoint, trim_path=(len(path) if path else 1))
            self[name] = item


class Dataset(Catalog):
    def __init__(self, dataset, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Catalog.__init__(self, dataset, token, base_url, flight_endpoint, kwargs.get('id', None),
                         kwargs.get('tag', None))
        for i in ('path', 'type', 'createdAt', 'format', 'approximateStatisticsAllowed'):
            self.meta[i] = kwargs.get(i, None)
        self.acceleration = Dict(refresh_policy=kwargs.get('accelerationRefreshPolicy', None), accelerations=list())

    def query(self):
        return self.sql("select * from {}")

    def sql(self, sql):
        return self._flight_endpoint(sql)


class PhysicalDataset(Dataset):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Dataset.__init__(self, "physical_dataset", token, base_url, flight_endpoint, **kwargs)


class VirtualDataset(Dataset):
    def __init__(self, token=None, base_url=None, flight_endpoint=None, **kwargs):
        Dataset.__init__(self, "virtual_dataset", token, base_url, flight_endpoint, **kwargs)
        self.sql = kwargs.get('sql', None)
        self.sqlContext = kwargs.get('sqlContext', None)
