# This file is part of the MapProxy project.
# Copyright (C) 2016 Omniscale <http://omniscale.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# azure-storage-blob==12.8.0


import os
import calendar
import hashlib
import sys
import threading

from io import BytesIO

from mapproxy.image import ImageSource
from mapproxy.cache import path
from mapproxy.cache.base import tile_buffer, TileCacheBase
from mapproxy.util import async_
from mapproxy.util.py import reraise_exception

try:
    from azure.storage.blob import BlobClient, ContainerClient, ContentSettings
except ImportError:
    BlobClient = None
    ContainerClient = None
    ContentSettings = None

import logging
log = logging.getLogger('mapproxy.cache.azureblob')

class AzureConnectionError(Exception):
    pass

class AzureBlobCache(TileCacheBase):
    def __init__(self, base_path, file_ext, directory_layout='tms', _concurrent_writer = 8, _concurrent_reader = 10, sas_token=None):
        super(AzureBlobCache, self).__init__()

        self.lock_cache_id = hashlib.md5(base_path.encode('utf-8')).hexdigest()
        self.conn = ContainerClient.from_container_url(sas_token)
        
        self.base_path = base_path
        self.file_ext = file_ext

        self._concurrent_writer = _concurrent_writer
        self._concurrent_reader = _concurrent_reader

        self._tile_location, _ = path.location_funcs(layout=directory_layout)

    def tile_key(self, tile):
        return self._tile_location(tile, self.base_path, self.file_ext).lstrip('/')

    def load_tile_metadata(self, tile):
        if tile.timestamp:
            return
        self.is_cached(tile)

    def is_cached(self, tile):
        if tile.is_missing():
            key = self.tile_key(tile)
            try:
                blob = self.conn.get_blob_client(key)
                properties = blob.get_blob_properties()
                tile.timestamp = calendar.timegm(properties.last_modified.timetuple())
                tile.size = properties.size
            except Exception as e:
                return False

        return True

    def load_tiles(self, tiles, with_metadata=True):
        p = async_.Pool(len(tiles))
        return all(p.map(self.load_tile, tiles))

    def load_tile(self, tile, with_metadata=True):
        if not tile.is_missing():
            return True

        key = self.tile_key(tile)
        log.debug('AzureBlob:load_tile, key: %s' % key)

        try:
            r  = self.conn.download_blob(key)
            tile.timestamp = calendar.timegm(r.properties.last_modified.timetuple())
            tile.size = r.properties.size
            tile.source = ImageSource(BytesIO(r.readall()))
        except Exception as e:
            return False

        return True

    def remove_tile(self, tile):
        key = self.tile_key(tile)
        log.debug('remove_tile, key: %s' % key)
        self.conn.delete_blob(key)

    def store_tiles(self, tiles):
        p = async_.Pool(min(self._concurrent_writer, len(tiles)))
        p.map(self.store_tile, tiles)

    def store_tile(self, tile):
        if tile.stored:
            return

        key = self.tile_key(tile)
        log.debug('AzureBlob: store_tile, key: %s' % key)

        with tile_buffer(tile) as buf:
            content_settings = ContentSettings(content_type='image/' + self.file_ext)
            self.conn.upload_blob(name=key, data=buf, overwrite=True, content_settings=content_settings)