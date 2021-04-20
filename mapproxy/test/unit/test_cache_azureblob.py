# This file is part of the MapProxy project.
# Copyright (C) 2011 Omniscale <http://omniscale.de>
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

import pytest

try:
    from mapproxy.cache.azureblob import AzureBlobCache
except ImportError:
    AzureBlobCache = None

from mapproxy.test.unit.test_cache_tile import TileCacheTestBase

@pytest.mark.skipif(not AzureBlobCache, reason="AzureBlobCache required for Azure Blob tests")
class TestAzureBlobCache(TileCacheTestBase):
    always_loads_metadata = True
    uses_utc = True

    def setup(self):
        TileCacheTestBase.setup(self)

        self.cache = AzureBlobCache('mapproxy',
            sas_token="https://lvbdevstorage.blob.core.windows.net/container?sp=racwdl&st=2021-04-20T13:39:03Z&se=2021-04-20T21:39:03Z&spr=https&sv=2020-02-10&sr=c&sig=5uxGRb%2FAjyr9KSZmHCKJdwoxhFybG3fCHhDIW0OgRIU%3D",
            file_ext='png',
            directory_layout='tms'
        )

    def teardown(self):
        TileCacheTestBase.teardown(self)

    @pytest.mark.parametrize('layout,tile_coord,key', [
        ['mp', (12345, 67890,  2), 'mycache/webmercator/02/0001/2345/0006/7890.png'],
        ['mp', (12345, 67890, 12), 'mycache/webmercator/12/0001/2345/0006/7890.png'],

        ['tc', (12345, 67890,  2), 'mycache/webmercator/02/000/012/345/000/067/890.png'],
        ['tc', (12345, 67890, 12), 'mycache/webmercator/12/000/012/345/000/067/890.png'],

        ['tms', (12345, 67890,  2), 'mycache/webmercator/2/12345/67890.png'],
        ['tms', (12345, 67890, 12), 'mycache/webmercator/12/12345/67890.png'],

        ['quadkey', (0, 0, 0), 'mycache/webmercator/.png'],
        ['quadkey', (0, 0, 1), 'mycache/webmercator/0.png'],
        ['quadkey', (1, 1, 1), 'mycache/webmercator/3.png'],
        ['quadkey', (12345, 67890, 12), 'mycache/webmercator/200200331021.png'],

        ['arcgis', (1, 2, 3), 'mycache/webmercator/L03/R00000002/C00000001.png'],
        ['arcgis', (9, 2, 3), 'mycache/webmercator/L03/R00000002/C00000009.png'],
        ['arcgis', (10, 2, 3), 'mycache/webmercator/L03/R00000002/C0000000a.png'],
        ['arcgis', (12345, 67890, 12), 'mycache/webmercator/L12/R00010932/C00003039.png'],
    ])

    def test_tile_key(self, layout, tile_coord, key):
        cache = AzureBlobCache('/mycache/webmercator', 'png', 
            directory_layout=layout,
            sas_token="https://lvbdevstorage.blob.core.windows.net/container?sp=racwdl&st=2021-04-20T13:39:03Z&se=2021-04-20T21:39:03Z&spr=https&sv=2020-02-10&sr=c&sig=5uxGRb%2FAjyr9KSZmHCKJdwoxhFybG3fCHhDIW0OgRIU%3D"
        )
        cache.store_tile(self.create_tile(tile_coord))
