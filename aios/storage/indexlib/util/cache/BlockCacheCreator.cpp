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
#include "indexlib/util/cache/BlockCacheCreator.h"

#include "indexlib/util/cache/CacheType.h"
#include "indexlib/util/cache/MemoryBlockCache.h"


using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, BlockCacheCreator);

BlockCache* BlockCacheCreator::Create(const BlockCacheOption& option)
{
    unique_ptr<BlockCache> blockCache;
    switch (GetCacheTypeFromStr(option.cacheType)) {
    case LRU:
        blockCache.reset(new MemoryBlockCache());
        break;
    default:
        AUTIL_LOG(ERROR, "unknown cacheType[%s]", option.cacheType.c_str());
        return nullptr;
    }
    if (!blockCache->Init(option)) {
        AUTIL_LOG(ERROR,
                  "init block cache failed with: "
                  "blockSize[%lu], blockSize[%lu], cacheType[%s], cache param [%s]",
                  option.blockSize, option.blockSize, option.cacheType.c_str(),
                  autil::legacy::ToJsonString(option.cacheParams, true).c_str());
        return nullptr;
    }
    return blockCache.release();
}
}} // namespace indexlib::util
