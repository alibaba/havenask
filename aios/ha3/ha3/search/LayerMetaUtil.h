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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class QueryLayerClause;
class RankClause;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

class LayerMetaUtil
{
public:
    LayerMetaUtil();
    ~LayerMetaUtil();
private:
    LayerMetaUtil(const LayerMetaUtil &);
    LayerMetaUtil& operator=(const LayerMetaUtil &);
public:
    static LayerMeta reverseRange(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, uint32_t totalDocCount);
    static LayerMeta reverseRangeWithEnd(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, uint32_t totalDocCount);
    static LayerMeta accumulateRanges(autil::mem_pool::Pool *pool, 
            const LayerMeta& layerMetaA,
            const LayerMeta& layerMetaB);
    static bool splitLayerMetas(autil::mem_pool::Pool *pool,
                               const LayerMetas &layerMetas,
                               const IndexPartitionReaderWrapperPtr &readerWrapper,
                               size_t totalWayCount,
                               std::vector<LayerMetasPtr> &splittedLayerMetas);
    static LayerMetasPtr createLayerMetas(
            const isearch::common::QueryLayerClause *queryLayerClause,
            const isearch::common::RankClause *rankClause,
            IndexPartitionReaderWrapper *readerWrapper,
            autil::mem_pool::Pool *pool);
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerMetaUtil> LayerMetaUtilPtr;

} // namespace search
} // namespace isearch

