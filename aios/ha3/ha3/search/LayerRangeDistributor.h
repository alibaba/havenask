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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Tracer.h"
#include "ha3/search/LayerMetas.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace isearch {
namespace search {

class LayerRangeDistributor
{
public:
    LayerRangeDistributor(LayerMetas *layerMetas,
                          autil::mem_pool::Pool *pool,
                          uint32_t docCount,
                          uint32_t rankSize,
                          common::Tracer *tracer = NULL);
    ~LayerRangeDistributor();
private:
    LayerRangeDistributor(const LayerRangeDistributor &);
    LayerRangeDistributor& operator=(const LayerRangeDistributor &);
public:
    void moveToNextLayer(uint32_t leftQuota);
    bool hasNextLayer() const {
        return _curLayerCursor < (int32_t)_layerMetas->size();
    }
    
    LayerMeta *getCurLayer(uint32_t &layer) {
        layer = _curLayerCursor;
        return &_curLayer;
    }
private:
    static void initLayerQuota(LayerMetas *layerMetas,uint32_t rankSize);
    static void proportionalLayerQuota(LayerMeta &layerMeta);
    static void averageLayerQuota(LayerMeta &layerMeta);
    static LayerMeta reverseRange(autil::mem_pool::Pool *pool,
                                  const LayerMeta &curLayer,
                                  uint32_t totalDocCount);
    static LayerMeta updateLayerMetaForNextLayer(autil::mem_pool::Pool *pool,
            const LayerMeta &curLayer, LayerMeta &nextLayer,
            uint32_t &leftQuota, uint32_t totalDocCount);
    static bool hasQuota(const LayerMeta &layerMeta);
private:
    LayerMetas *_layerMetas;
    autil::mem_pool::Pool *_pool;
    LayerMeta _seekedRange;
    LayerMeta _curLayer;
    uint32_t _docCount;
    uint32_t _leftQuota;
    int32_t _curLayerCursor;
    common::Tracer *_tracer;
private:
    friend class LayerRangeDistributorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerRangeDistributor> LayerRangeDistributorPtr;

} // namespace search
} // namespace isearch

