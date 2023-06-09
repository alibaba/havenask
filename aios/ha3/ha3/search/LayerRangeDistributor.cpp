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
#include "ha3/search/LayerRangeDistributor.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <string>

#include "autil/mem_pool/PoolVector.h"
#include "indexlib/indexlib.h"

#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/search/LayerMetas.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, LayerRangeDistributor);

class DocIdRangeMetaComp {
public:
    bool operator () (const DocIdRangeMeta &lft, const DocIdRangeMeta &rht) {
        return lft.begin < rht.begin;
    }
};

LayerRangeDistributor::LayerRangeDistributor(LayerMetas *layerMetas,
        autil::mem_pool::Pool *pool, uint32_t docCount,
        uint32_t rankSize, common::Tracer *tracer)
    : _layerMetas(layerMetas)
    , _pool(pool)
    , _seekedRange(pool)
    , _curLayer(pool)
    , _docCount(docCount)
    , _leftQuota(0)
    , _curLayerCursor(-1)
    , _tracer(tracer)
{
    initLayerQuota(_layerMetas, rankSize);
    moveToNextLayer(0);
}

LayerRangeDistributor::~LayerRangeDistributor() {
}

void LayerRangeDistributor::initLayerQuota(
        LayerMetas *layerMetas, uint32_t rankSize)

{
    for (size_t i = 0; i < layerMetas->size(); ++i) {
        LayerMeta &layerMeta = (*layerMetas)[i];
        layerMeta.quota = min(rankSize, layerMeta.quota);
        layerMeta.maxQuota = max(rankSize, layerMeta.quota);
        rankSize -= layerMeta.quota;
    }
}

void LayerRangeDistributor::moveToNextLayer(uint32_t leftQuota) {
    _leftQuota += leftQuota;
    for (; _curLayerCursor + 1 < (int32_t)_layerMetas->size(); ++_curLayerCursor) {
        for (size_t i = 0; i < _curLayer.size(); ++i) {
            _seekedRange.push_back(_curLayer[i]);
        }
        sort(_seekedRange.begin(), _seekedRange.end(), DocIdRangeMetaComp());
        _curLayer = updateLayerMetaForNextLayer(_pool, _seekedRange,
                (*_layerMetas)[_curLayerCursor + 1], _leftQuota, _docCount);

        if (_curLayer.quotaType == QT_AVERAGE) {
            averageLayerQuota(_curLayer);
        } else {
            proportionalLayerQuota(_curLayer);
        }

        if (_curLayer.size() > 0 && hasQuota(_curLayer)) {
            break;
        }
        _leftQuota += _curLayer.quota;
    }
    REQUEST_TRACE(DEBUG, "current layer meta: [%s]", _curLayer.toString().c_str());
    ++_curLayerCursor;
}

void LayerRangeDistributor::averageLayerQuota(LayerMeta &layerMeta) {
    if (layerMeta.size() == 0
        || layerMeta.quota == 0
        || layerMeta.quotaMode == QM_PER_LAYER)
    {
        return;
    }

    uint32_t averageQuota = (uint32_t) ((double)layerMeta.quota / layerMeta.size());
    uint32_t remainQuota = layerMeta.quota;
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        DocIdRangeMeta &meta = layerMeta[i];
        meta.quota = averageQuota;
        remainQuota -= meta.quota;
    }
    layerMeta[0].quota += remainQuota;
    layerMeta.quota = 0;
}

void LayerRangeDistributor::proportionalLayerQuota(LayerMeta &layerMeta) {
    if (layerMeta.size() == 0
        || layerMeta.quota == 0
        || layerMeta.quotaMode == QM_PER_LAYER)
    {
        return;
    }

    uint32_t remainQuota = layerMeta.quota;
    docid_t totalRange = 0;
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        DocIdRangeMeta &meta = layerMeta[i];
        totalRange += meta.end - meta.begin + 1;
    }
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        DocIdRangeMeta &meta = layerMeta[i];
        docid_t range = meta.end - meta.begin + 1;
        meta.quota = (uint32_t) ((double)range * layerMeta.quota / totalRange);
        remainQuota -= meta.quota;
    }
    layerMeta[0].quota += remainQuota;
    layerMeta.quota = 0;
}

bool LayerRangeDistributor::hasQuota(const LayerMeta &layerMeta) {
    if (layerMeta.quota > 0) {
        assert(layerMeta.quotaMode == QM_PER_LAYER);
        return true;
    }
    for (size_t i = 0; i < layerMeta.size(); ++i) {
        if (layerMeta[i].quota > 0) {
            return true;
        }
    }
    return false;
}

LayerMeta LayerRangeDistributor::reverseRange(autil::mem_pool::Pool *pool,
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

LayerMeta LayerRangeDistributor::updateLayerMetaForNextLayer(
        autil::mem_pool::Pool *pool, const LayerMeta &curLayer,
        LayerMeta &nextLayer, uint32_t &leftQuota, uint32_t totalDocCount)
{
    LayerMeta targetLayer(pool);
    size_t cur = 0;
    size_t next = 0;
    LayerMeta range = reverseRange(pool, curLayer, totalDocCount);

    while (cur < range.size() && next < nextLayer.size()) {
        if (range[cur].end < nextLayer[next].begin) {
            ++cur;
            continue;
        }
        if (range[cur].begin > nextLayer[next].end) {
            leftQuota += nextLayer[next].quota;
            ++next;
            continue;
        }
        docid_t begin = max(range[cur].begin, nextLayer[next].begin);
        docid_t end = min(range[cur].end, nextLayer[next].end);
        leftQuota += nextLayer[next].quota;
        nextLayer[next].quota = 0;
        if (end >= begin) {
            targetLayer.push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, 0));
        }
        if (range[cur].end > nextLayer[next].end) {
            ++next;
        } else {
            ++cur;
        }
    }

    // if (leftQuota > 0 && targetLayer.size() > 0) {
    //     targetLayer[0].quota += leftQuota;
    //     leftQuota = 0;
    // }
    targetLayer.quota = nextLayer.quota + leftQuota;
    targetLayer.maxQuota = std::max(nextLayer.maxQuota, targetLayer.quota);
    targetLayer.quotaMode = nextLayer.quotaMode;
    targetLayer.quotaType = nextLayer.quotaType;
    leftQuota = 0;

    return targetLayer;
}

} // namespace search
} // namespace isearch
