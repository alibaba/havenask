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
#include "ha3/search/LayerMetaUtil.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/RankClause.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/LayerMetasCreator.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace common {
class QueryLayerClause;
struct RankHint;
}  // namespace common
}  // namespace isearch

using namespace std;
using namespace autil;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, LayerMetaUtil);
using namespace isearch::common;

LayerMetaUtil::LayerMetaUtil() {
}

LayerMetaUtil::~LayerMetaUtil() {
}

LayerMeta LayerMetaUtil::reverseRange(autil::mem_pool::Pool *pool,
        const LayerMeta &curLayer, uint32_t totalDocCount)
{
    docid_t begin = 0;
    LayerMeta result(pool);
    for (size_t i = 0; i < curLayer.size(); ++i) {
        if (curLayer[i].begin > begin) {
            result.push_back(DocIdRangeMeta(begin, curLayer[i].begin - 1,
                            DocIdRangeMeta::OT_UNKNOWN, 0));
        }
        begin = curLayer[i].nextBegin;
    }
    result.push_back(DocIdRangeMeta(begin, totalDocCount - 1, DocIdRangeMeta::OT_UNKNOWN, 0));
    return result;
}

LayerMeta LayerMetaUtil::reverseRangeWithEnd(autil::mem_pool::Pool *pool,
        const LayerMeta &curLayer, uint32_t totalDocCount)
{
    docid_t begin = 0;
    LayerMeta result(pool);
    for (size_t i = 0; i < curLayer.size(); ++i) {
        if (curLayer[i].begin > begin) {
            result.push_back(DocIdRangeMeta(begin, curLayer[i].begin - 1,
                            DocIdRangeMeta::OT_UNKNOWN, 0));
        }
        begin = curLayer[i].end + 1;
    }
    result.push_back(DocIdRangeMeta(begin, totalDocCount - 1, DocIdRangeMeta::OT_UNKNOWN, 0));
    return result;
}

LayerMeta LayerMetaUtil::accumulateRanges(autil::mem_pool::Pool *pool,
        const LayerMeta& layerMetaA,
        const LayerMeta& layerMetaB)
{
    LayerMeta tmpLayer(pool);
    for (size_t i = 0; i < layerMetaA.size(); ++i) {
        tmpLayer.push_back(layerMetaA[i]);
    }
    for (size_t i = 0; i < layerMetaB.size(); ++i) {
        tmpLayer.push_back(layerMetaB[i]);
    }
    if (tmpLayer.empty()) {
        return tmpLayer;
    }
    sort(tmpLayer.begin(), tmpLayer.end(), DocIdRangeMetaCompare());
    LayerMeta resultLayer(pool);
    size_t i = 0;
    int32_t begin = tmpLayer[0].begin;
    int32_t end = tmpLayer[0].end;
    while (i + 1 < tmpLayer.size()) {
        DocIdRangeMeta& meta = tmpLayer[i + 1];
        if (end + 1 > meta.begin) {
            if(end + 1 <= meta.end) {
                end = meta.end;
            }
        } else {
            resultLayer.push_back(DocIdRangeMeta(begin, end));
            begin = meta.begin;
            end = meta.end;
        }
        ++i;
    }
    resultLayer.push_back(DocIdRangeMeta(begin, end));
    return resultLayer;
}

bool LayerMetaUtil::splitLayerMetas(
        autil::mem_pool::Pool *pool,
        const LayerMetas &layerMetas,
        const IndexPartitionReaderWrapperPtr &readerWrapper,
        size_t totalWayCount,
        vector<LayerMetasPtr> &splitLayerMetas)
{
    splitLayerMetas.clear();
    splitLayerMetas.reserve(totalWayCount);
    for (size_t i = 0; i < totalWayCount ; i++) {
        LayerMetasPtr layerMetasTmp(POOL_NEW_CLASS(pool, LayerMetas, pool),
                [](LayerMetas* p) {
                    POOL_DELETE_CLASS(p);
                });
        splitLayerMetas.push_back(layerMetasTmp);
    }
    for (auto &originLayerMeta : layerMetas) {
        indexlib::DocIdRangeVector rangeVec;
        for (auto &docIdRangeMeta : originLayerMeta) {
            rangeVec.push_back(make_pair(
                            docIdRangeMeta.begin, docIdRangeMeta.end + 1));
        }
        vector<indexlib::DocIdRangeVector> splitRangeVec;
        if (!readerWrapper->getPartedDocIdRanges(
                        rangeVec, totalWayCount, splitRangeVec))
        {
            return false;
        }
        if (totalWayCount < splitRangeVec.size()) {
            return false;
        }
        while (splitRangeVec.size() < totalWayCount) {
            indexlib::DocIdRangeVector emptyRangeVec;
            splitRangeVec.push_back(emptyRangeVec);
        }
        for (size_t idx=0; idx<splitRangeVec.size(); ++idx) {
            LayerMeta newLayerMeta(pool);
            for (auto newRange : splitRangeVec[idx]) {
                newRange.second--;
                newLayerMeta.push_back(newRange);
            }
            newLayerMeta.quota = originLayerMeta.quota;
            newLayerMeta.maxQuota = originLayerMeta.maxQuota;
            newLayerMeta.quotaMode = originLayerMeta.quotaMode;
            newLayerMeta.needAggregate = originLayerMeta.needAggregate;
            newLayerMeta.quotaType = originLayerMeta.quotaType;
            newLayerMeta.initRangeString();
            splitLayerMetas[idx]->push_back(newLayerMeta);
        }
    }
    return true;
}

LayerMetasPtr LayerMetaUtil::createLayerMetas(
        const QueryLayerClause *queryLayerClause,
        const RankClause *rankClause,
        IndexPartitionReaderWrapper *readerWrapper,
        autil::mem_pool::Pool *pool)
{
    LayerMetasCreator layerMetasCreator;
    const RankHint *rankHint = NULL;
    if (rankClause) {
        rankHint = rankClause->getRankHint();
    }

    layerMetasCreator.init(pool, readerWrapper, rankHint);
    LayerMetasPtr layerMetas(layerMetasCreator.create(queryLayerClause),
                                [](LayerMetas* p) {
                POOL_DELETE_CLASS(p);
            });
    return layerMetas;
}

} // namespace search
} // namespace isearch
