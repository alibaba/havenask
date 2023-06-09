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
#include "ha3/search/CacheResult.h"

#include <assert.h>
#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/common/Result.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "ha3/util/XMLFormatUtil.h"
#include "indexlib/index/partition_info.h"
#include "matchdoc/CommonDefine.h"
#include "suez/turing/expression/common.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace suez::turing;
using namespace isearch::util;
using namespace isearch::common;

namespace autil {
DECLARE_SIMPLE_TYPE(indexlib::index::PartitionInfoHint);
}

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, CacheResult);

CacheResult::CacheResult()
    : _result(NULL)
    , _isTruncated(false)
    , _useTruncateOptimizer(false)
{
}

CacheResult::~CacheResult() {
    DELETE_AND_SET_NULL(_result);
}

common::Result *CacheResult::stealResult() {
    common::Result *tmp = _result;
    _result = NULL;
    return tmp;
}

void CacheResult::serialize(autil::DataBuffer &dataBuffer) const  {
    serializeHeader(dataBuffer);
    serializeResult(dataBuffer);
}

void CacheResult::serializeHeader(autil::DataBuffer &dataBuffer) const  {
    dataBuffer.write(_header.expireTime);
    dataBuffer.write(_header.minScoreFilter);
    dataBuffer.write(_header.time);
}

void CacheResult::serializeResult(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_partInfoHint);
    dataBuffer.write(_gids);
    dataBuffer.write(_isTruncated);
    dataBuffer.write(_useTruncateOptimizer);
    bool hasSubDoc = !_subDocGids.empty();
    dataBuffer.write(hasSubDoc);
    if (hasSubDoc) {
        dataBuffer.write(_subDocGids);
    }

    // write result
    uint8_t originalSerializeLevel = SL_NONE;
    if (_result) {
        // skip some reference which dose not need serialize value
        // such as DocIdentifier/ip/FullIndexVersion...
        originalSerializeLevel = _result->getSerializeLevel();
        _result->setSerializeLevel(SL_CACHE);
    }
    dataBuffer.write(_result);
    if (_result) {
        _result->setSerializeLevel(originalSerializeLevel);
    }
}

void CacheResult::deserialize(autil::DataBuffer &dataBuffer,
                              autil::mem_pool::Pool *pool)
{
    deserializeHeader(dataBuffer);
    deserializeResult(dataBuffer, pool);
}

void CacheResult::deserializeHeader(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_header.expireTime);
    dataBuffer.read(_header.minScoreFilter);
    dataBuffer.read(_header.time);
}

void CacheResult::deserializeResult(autil::DataBuffer &dataBuffer,
                                    autil::mem_pool::Pool *pool)
{
    dataBuffer.read(_partInfoHint);
    dataBuffer.read(_gids);

    dataBuffer.read(_isTruncated);
    dataBuffer.read(_useTruncateOptimizer);

    bool hasSubDoc = false;
    dataBuffer.read(hasSubDoc);
    if (hasSubDoc) {
        dataBuffer.read(_subDocGids);
    }

    bool bResultExist = false;
    dataBuffer.read(bResultExist);
    if (!bResultExist) {
        return;
    }

    _result = new common::Result();
    assert(_result != NULL);
    uint8_t originalSerializeLevel = _result->getSerializeLevel();
    _result->setSerializeLevel(SL_CACHE);
    _result->deserialize(dataBuffer, pool);
    _result->setSerializeLevel(originalSerializeLevel);
}

} // namespace search
} // namespace isearch
