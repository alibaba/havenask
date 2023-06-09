/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfo.h"

namespace indexlibv2::index {

class SKeyCollectInfoPool
{
public:
    SKeyCollectInfoPool() : _cursor(0) {}

    ~SKeyCollectInfoPool()
    {
        for (size_t i = 0; i < _collectInfoPool.size(); ++i) {
            assert(_collectInfoPool[i]);
            POOL_COMPATIBLE_DELETE_CLASS((&_pool), _collectInfoPool[i]);
            _collectInfoPool[i] = nullptr;
        }
    }

    SKeyCollectInfo* CreateCollectInfo()
    {
        while (_collectInfoPool.size() <= _cursor) {
            SKeyCollectInfo* info = POOL_COMPATIBLE_NEW_CLASS((&_pool), SKeyCollectInfo);
            _collectInfoPool.push_back(info);
        }
        return _collectInfoPool[_cursor++];
    }

    void Reset() { _cursor = 0; }

private:
    std::vector<SKeyCollectInfo*> _collectInfoPool;
    size_t _cursor;
    autil::mem_pool::Pool _pool;
};

} // namespace indexlibv2::index
