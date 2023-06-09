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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/misc/common.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class LayerDescription;
class LayerKeyRange;
class QueryLayerClause;
struct RankHint;
}  // namespace common
namespace search {
class IndexPartitionReaderWrapper;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

class LayerMetasCreator
{
public:
    LayerMetasCreator();
    ~LayerMetasCreator();
private:
    LayerMetasCreator(const LayerMetasCreator &);
    LayerMetasCreator& operator=(const LayerMetasCreator &);
public:
    void init(autil::mem_pool::Pool *pool,
              IndexPartitionReaderWrapper* readerPtr,
              const common::RankHint *rankHint = NULL);
    LayerMetas* create(const common::QueryLayerClause* layerClause) const;
private:
    LayerMetas* createLayerMetas(const common::QueryLayerClause* layerClause) const;
    LayerMetas* createDefaultLayerMetas() const;
    LayerMeta createFullRange() const;
    LayerMeta createLayerMeta(const common::LayerDescription *desc,
                              const LayerMeta &visitedRange) const;
private:
    LayerMeta convertSortAttrRange(
            const common::LayerDescription* layerDesc, int32_t from,
            int32_t to,const LayerMeta& lastRange) const;
    LayerMeta convertRangeKeyWord(const common::LayerKeyRange *range,
                                  LayerMeta &lastRange,
                                  const LayerMeta &visitedRange) const;
private:
    LayerMeta segmentidLayerConvert(const common::LayerKeyRange *layerRange) const;
    LayerMeta docidLayerConvert(const common::LayerKeyRange *layerRange) const;
    LayerMeta orderedDocIdConvert() const;
    LayerMeta unOrderedDocIdConvert() const;
    LayerMeta percentConvert(const LayerMeta &lastRange,
                             const common::LayerKeyRange *range) const;
    LayerMeta otherConvert(const LayerMeta &lastRange,
                           const LayerMeta &visitedRange) const;
private:
    LayerMeta interSectRanges(const LayerMeta& layerMetaA,
                              const LayerMeta& layerMetaB) const;
private:
    QuotaMode decideQuotaMode(const common::LayerKeyRange *range,
                              size_t dimenCount, bool containUnSortedRange) const;
private:
    static int32_t convertNumberToRange(const std::string &numStr, int32_t maxNum);
    static indexlib::table::DimensionDescriptionVector convert2DimenDescription(
            const common::LayerDescription* layerDesc, int32_t from, int32_t to);
    static bool needAggregate(const common::LayerDescription *desc);

private:
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapper *_readerPtr;
    const common::RankHint *_rankHint;
private:
    AUTIL_LOG_DECLARE();
    friend class LayerMetasCreatorTest;
};

typedef std::shared_ptr<LayerMetasCreator> LayerMetasCreatorPtr;

} // namespace search
} // namespace isearch
