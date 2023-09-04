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
#include "ha3/search/DefaultLayerMetaUtil.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <stdint.h>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, DefaultLayerMetaUtil);

DefaultLayerMetaUtil::DefaultLayerMetaUtil() {}

DefaultLayerMetaUtil::~DefaultLayerMetaUtil() {}

LayerMetas *DefaultLayerMetaUtil::createDefaultLayerMetas(autil::mem_pool::Pool *pool,
                                                          IndexPartitionReaderWrapper *reader) {
    LayerMetas *layerMetas = POOL_NEW_CLASS(pool, LayerMetas, pool);
    LayerMeta layerMeta = createFullRange(pool, reader);
    layerMeta.quota = numeric_limits<uint32_t>::max();
    layerMetas->push_back(layerMeta);
    return layerMetas;
}

LayerMeta DefaultLayerMetaUtil::createFullRange(autil::mem_pool::Pool *pool,
                                                IndexPartitionReaderWrapper *reader) {
    const shared_ptr<PartitionInfoWrapper> &partInfo = reader->getPartitionInfo();
    LayerMeta layerMeta(pool);

    indexlib::DocIdRangeVector ranges;
    if (partInfo->GetOrderedDocIdRanges(ranges)) {
        for (size_t i = 0; i < ranges.size(); ++i) {
            indexlib::DocIdRange &range = ranges[i];
            --range.second;
            if (range.second >= range.first) {
                layerMeta.push_back({range, DocIdRangeMeta::OT_ORDERED});
            }
        }
    }
    indexlib::DocIdRange unSortedRange;
    if (partInfo->GetUnorderedDocIdRange(unSortedRange)) {
        --unSortedRange.second;
        if (unSortedRange.second >= unSortedRange.first) {
            layerMeta.push_back({unSortedRange, DocIdRangeMeta::OT_UNORDERED});
        }
    }
    return layerMeta;
}

} // namespace search
} // namespace isearch
