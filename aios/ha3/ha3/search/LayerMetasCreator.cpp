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
#include "ha3/search/LayerMetasCreator.h"

#include <assert.h>
#include <math.h>
#include <algorithm>
#include <iosfwd>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/LayerDescription.h"
#include "ha3/common/QueryLayerClause.h"
#include "ha3/isearch.h"
#include "ha3/search/DefaultLayerMetaUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetaUtil.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/LayerValidator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_define.h"

namespace isearch {
namespace common {
struct RankHint;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, LayerMetasCreator);

using namespace indexlib::partition;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace isearch::common;
using namespace std;
using namespace autil;

LayerMetasCreator::LayerMetasCreator()
    : _pool(NULL)
    , _rankHint(NULL)
{
}

LayerMetasCreator::~LayerMetasCreator() {
}

void LayerMetasCreator::init(autil::mem_pool::Pool *pool,
                             IndexPartitionReaderWrapper* readerPtr,
                             const RankHint *rankHint)
{
    _pool = pool;
    _readerPtr = readerPtr;
    _rankHint = rankHint;
}

LayerMetas* LayerMetasCreator::create(const QueryLayerClause* layerClause) const {
    LayerMetas *layerMetas = createLayerMetas(layerClause);
    return layerMetas;
}

LayerMetas* LayerMetasCreator::createLayerMetas(const QueryLayerClause* layerClause) const {
    if (!layerClause || layerClause->getLayerCount() == 0) {
        return DefaultLayerMetaUtil::createDefaultLayerMetas(_pool, _readerPtr);
    }
    LayerMetas* layerMetas = POOL_NEW_CLASS(_pool, LayerMetas, _pool);
    size_t layerCount = layerClause->getLayerCount();
    LayerMeta visitedRange(_pool);          // keep visited range for %other.
    for (size_t i = 0; i < layerCount; ++i) {
        LayerDescription* layerDesc = layerClause->getLayerDescription(i);
        layerMetas->push_back(createLayerMeta(layerDesc, visitedRange));
        visitedRange = LayerMetaUtil::accumulateRanges(_pool,
                layerMetas->back(), visitedRange);
    }
    if (layerMetas->empty()) {
        return DefaultLayerMetaUtil::createDefaultLayerMetas(_pool, _readerPtr);
    }
    return layerMetas;
}

// each layer range key create a new range based on layerMeta.
// layerMeta is full range in the beginning.
// sorted range must be continuous for indexlib api.
// set other metas(like quota,quotaMode etc.) in the end.
LayerMeta LayerMetasCreator::createLayerMeta(const LayerDescription *desc,
        const LayerMeta &visitedRange) const
{
    LayerMeta layerMeta = DefaultLayerMetaUtil::createFullRange(_pool, _readerPtr);
    LayerKeyRange emptyRange;

    size_t from = 1;
    size_t to = 0;
    bool containUnSortedRange = false;
    for (size_t i = 0; i < desc->getLayerRangeCount(); ++i) {
        LayerKeyRange *range = desc->getLayerRange(i);
        // if don't have attr name in this range
        // need validate dimenCount is to - from + 1 = 0
        from = 1;
        to = 0;
        if (!range->isRangeKeyWord()) {
            //sortAttr range must be continuous
            from = i;
            for (;i < desc->getLayerRangeCount(); ++i) {
                range = desc->getLayerRange(i);
                if (range->isRangeKeyWord()) {
                    break;
                }
            }
            to = i - 1;
            layerMeta = convertSortAttrRange(desc, from, to, layerMeta);
        }
        if (i >= desc->getLayerRangeCount()) {
            break;
        }
        assert(range->isRangeKeyWord());
        layerMeta = convertRangeKeyWord(range, layerMeta, visitedRange);
        if (range->attrName == LAYERKEY_UNSORTED) {
            containUnSortedRange = true;
        }
    }
    int32_t dimenCount = to - from + 1;
    LayerKeyRange *lastAttrDimen = dimenCount > 0 ? desc->getLayerRange(to) : NULL;
    layerMeta.quota = desc->getQuota();
    if (layerMeta.quotaMode == QM_PER_DOC) {
        // default mode is per_doc
        layerMeta.quotaMode = decideQuotaMode(lastAttrDimen, dimenCount, containUnSortedRange);
    }
    layerMeta.needAggregate = needAggregate(desc);
    layerMeta.quotaType = desc->getQuotaType();
    if (layerMeta.quotaType == QT_QUOTA) {
        layerMeta.quota *= layerMeta.size();
        layerMeta.quotaType = QT_AVERAGE;
    }
    return layerMeta;
}

bool LayerMetasCreator::needAggregate(const LayerDescription *desc) {
    if (desc->getLayerRangeCount() != 1) {
        return true;
    }
    LayerKeyRange *keyRange = desc->getLayerRange(0);
    return keyRange->attrName != LAYERKEY_OTHER;
}

indexlib::table::DimensionDescriptionVector LayerMetasCreator::convert2DimenDescription(
        const LayerDescription* layerDesc,
        int32_t from, int32_t to)
{
    indexlib::table::DimensionDescriptionVector dimens;
    for (int32_t i = from; i <= to; ++i) {
        LayerKeyRange* keyRange = layerDesc->getLayerRange(i);
        std::shared_ptr<indexlib::table::DimensionDescription> dimen(new indexlib::table::DimensionDescription);
        dimen->name = keyRange->attrName;
        dimen->values = keyRange->values;
        for (size_t j = 0; j < keyRange->ranges.size(); ++j) {
            dimen->ranges.push_back(indexlib::table::DimensionDescription::Range(keyRange->ranges[j].from,
                            keyRange->ranges[j].to));
        }
        dimens.push_back(dimen);
    }
    return dimens;
}

LayerMeta LayerMetasCreator::convertSortAttrRange(
        const LayerDescription* layerDesc, int32_t from,
        int32_t to,const LayerMeta& lastRange) const
{

    LayerMeta layerMeta(_pool);
    indexlib::table::DimensionDescriptionVector dimens = convert2DimenDescription(layerDesc, from, to);
    for (size_t i = 0; i < lastRange.size(); ++i) {
        indexlib::DocIdRange rangeLimit(lastRange[i].begin, lastRange[i].end + 1);
        if (rangeLimit.second < rangeLimit.first) {
            continue;
        }
        indexlib::DocIdRangeVector resultRanges;
        if (!_readerPtr->getSortedDocIdRanges(
                        dimens, rangeLimit, resultRanges))
        {
            // validate failed, make quotaMode per layer to make sure seek all doc
            layerMeta = lastRange;
            layerMeta.quotaMode = QM_PER_LAYER;
            return layerMeta;
        }
        for (size_t j = 0; j < resultRanges.size(); ++j) {
            indexlib::DocIdRange range(resultRanges[j].first,
                    resultRanges[j].second - 1);
            if (range.second >= range.first) {
                layerMeta.push_back(range);
            }
        }
    }
    return layerMeta;
}

LayerMeta LayerMetasCreator::convertRangeKeyWord(const LayerKeyRange *range,
        LayerMeta &lastRange, const LayerMeta &visitedRange) const
{

    if (range->attrName == LAYERKEY_DOCID) {
        return interSectRanges(lastRange , docidLayerConvert(range));
    } else if (range->attrName == LAYERKEY_OTHER) {
        return interSectRanges(lastRange, otherConvert(lastRange, visitedRange));
    } else if (range->attrName == LAYERKEY_SEGMENTID) {
        return interSectRanges(lastRange , segmentidLayerConvert(range));
    } else if (range->attrName == LAYERKEY_SORTED) {
        return interSectRanges(lastRange, orderedDocIdConvert());
    } else if (range->attrName == LAYERKEY_UNSORTED) {
        return interSectRanges(lastRange, unOrderedDocIdConvert());
    } else if (range->attrName == LAYERKEY_PERCENT) {
        return percentConvert(lastRange, range);
    }
    assert(false);
    return LayerMeta(_pool);
}

LayerMeta LayerMetasCreator::otherConvert(const LayerMeta &lastRange,
        const LayerMeta &visitedRange) const
{
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    uint32_t totalDocCount = partitionInfo->GetTotalDocCount();
    return interSectRanges(lastRange, LayerMetaUtil::reverseRangeWithEnd(_pool, visitedRange, totalDocCount));
}

//percent must not discard any doc so it must be [from, to)
LayerMeta LayerMetasCreator::percentConvert(const LayerMeta &lastRange,
        const LayerKeyRange *range) const
{
    int32_t totalDocCount = 0;
    for (size_t i = 0; i < lastRange.size(); i++) {
        int32_t rangeSize = lastRange[i].end - lastRange[i].begin + 1;
        totalDocCount += rangeSize;
    }
    LayerMeta layerMeta(_pool);
    for (size_t i = 0; i < range->ranges.size(); ++i) {
        int32_t from;
        if (range->ranges[i].from == "INFINITE") {
            from = 0;
        } else {
            from = convertNumberToRange(range->ranges[i].from, 101);
        }
        int32_t to = convertNumberToRange(range->ranges[i].to, 101);

        // [from, to) anyway
        int32_t fromDocCount = int32_t(ceil(double(totalDocCount) * from / 100));
        int32_t toDocCount = int32_t(ceil(double(totalDocCount) * to / 100)) - 1;

        for (size_t j = 0; j < lastRange.size(); ++j) {
            docid_t virtualBegin = lastRange[j].begin + fromDocCount;
            docid_t virtualEnd = lastRange[j].begin + toDocCount;

            docid_t actBegin = max(virtualBegin, lastRange[j].begin);
            docid_t actEnd = min(virtualEnd, lastRange[j].end);

            if (actBegin <= actEnd) {
                layerMeta.push_back(DocIdRangeMeta(actBegin, actEnd));
            }

            if (fromDocCount == 0 && toDocCount == 0) {
                break;
            }

            int32_t rangeSize = lastRange[j].end - lastRange[j].begin + 1;
            fromDocCount = max(0, fromDocCount - rangeSize);
            toDocCount = max(-1, toDocCount - rangeSize);
        }
    }
    LayerMeta emptyLayerMeta(_pool);
    return LayerMetaUtil::accumulateRanges(_pool, layerMeta, emptyLayerMeta);
}


LayerMeta LayerMetasCreator::unOrderedDocIdConvert() const {
    LayerMeta layerMeta(_pool);
    indexlib::DocIdRange unSortedRange;
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    partitionInfo->GetUnorderedDocIdRange(unSortedRange);
    --unSortedRange.second;
    if (unSortedRange.second >= unSortedRange.first) {
        layerMeta.push_back(unSortedRange);
    }
    return layerMeta;
}

LayerMeta LayerMetasCreator::orderedDocIdConvert() const {
    LayerMeta layerMeta(_pool);
    indexlib::DocIdRangeVector ranges;
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    partitionInfo->GetOrderedDocIdRanges(ranges);
    for (uint32_t i = 0; i < ranges.size(); ++i) {
        DocIdRangeMeta rangeMeta(ranges[i].first, ranges[i].second - 1);
        layerMeta.push_back(rangeMeta);
    }
    return layerMeta;
}

LayerMeta LayerMetasCreator::docidLayerConvert(const LayerKeyRange *layerRange) const {
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    docid_t totalDocCount = partitionInfo->GetTotalDocCount();
    LayerMeta layerMeta(_pool);
    for (size_t i = 0; i < layerRange->ranges.size(); ++i) {
        // [from , to)
        docid_t from;
        if (layerRange->ranges[i].from == "INFINITE") {
            from = 0;
        } else {
            from = convertNumberToRange(
                    layerRange->ranges[i].from, totalDocCount);
        }
        docid_t to = convertNumberToRange(
                layerRange->ranges[i].to, totalDocCount) - 1;
        if (to >= from) {
            layerMeta.push_back(DocIdRangeMeta(from, to));
        }
    }
    for (size_t i = 0; i < layerRange->values.size(); ++i) {
        docid_t value = convertNumberToRange(layerRange->values[i], totalDocCount);
        layerMeta.push_back(DocIdRangeMeta(value,value));
    }
    LayerMeta emptyLayerMeta(_pool);
    return LayerMetaUtil::accumulateRanges(_pool, layerMeta, emptyLayerMeta);
}

LayerMeta LayerMetasCreator::segmentidLayerConvert(const LayerKeyRange *layerRange) const
{
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    segmentid_t segmentCount = partitionInfo->GetSegmentCount();
    LayerMeta layerMeta(_pool);

    docid_t totalDocCount = partitionInfo->GetTotalDocCount();
    std::vector<docid_t> baseDocIds = partitionInfo->GetBaseDocIds();
    assert(baseDocIds.size() == (size_t)segmentCount);
    baseDocIds.push_back(totalDocCount);

    for (size_t i = 0; i < layerRange->ranges.size(); ++i) {
        // [from, to)
        int32_t from;
        if (layerRange->ranges[i].from == "INFINITE") {
            from = 0;
        } else {
            from = convertNumberToRange(layerRange->ranges[i].from, segmentCount);
        }
        int32_t to = convertNumberToRange(layerRange->ranges[i].to, segmentCount) - 1;
        if (to >= from) {
            docid_t fromDocid = baseDocIds[from];
            docid_t toDocid = baseDocIds[to+1] - 1;
            if (toDocid >= fromDocid) {
                layerMeta.push_back(DocIdRangeMeta(fromDocid, toDocid));
            }
        }
    }
    for (size_t i = 0; i < layerRange->values.size(); ++i) {
        docid_t value = convertNumberToRange(layerRange->values[i], segmentCount);
        docid_t fromDocid = baseDocIds[value];
        docid_t toDocid = baseDocIds[value+1] - 1;
        if (toDocid >= fromDocid) {
            layerMeta.push_back(DocIdRangeMeta(fromDocid, toDocid));
        }
    }
    LayerMeta emptyLayerMeta(_pool);
    return LayerMetaUtil::accumulateRanges(_pool, layerMeta, emptyLayerMeta);
}

int32_t LayerMetasCreator::convertNumberToRange(const string &numStr, int32_t maxNum) {
    if(numStr == "INFINITE") {
        return maxNum;
    }
    int32_t num = StringUtil::fromString<int32_t>(numStr.c_str());
    assert(num >= 0);
    if (num >= maxNum) {
        num = maxNum;
    }

    return num;
}

LayerMeta LayerMetasCreator::interSectRanges(const LayerMeta& layerMetaA,
        const LayerMeta& layerMetaB) const
{
    LayerMeta resultRange(_pool);
    size_t aCursor = 0;
    size_t bCursor = 0;
    while(aCursor < layerMetaA.size() && bCursor < layerMetaB.size()) {
        if (layerMetaA[aCursor].end < layerMetaB[bCursor].begin) {
            ++aCursor;
            continue;
        }
        if (layerMetaA[aCursor].begin > layerMetaB[bCursor].end) {
            ++bCursor;
            continue;
        }
        docid_t begin = max(layerMetaA[aCursor].begin, layerMetaB[bCursor].begin);
        docid_t end = min(layerMetaA[aCursor].end, layerMetaB[bCursor].end);
        if (end >= begin) {
            resultRange.push_back(DocIdRangeMeta(begin, end));
        }
        if (layerMetaA[aCursor].end > layerMetaB[bCursor].end) {
            ++bCursor;
        } else {
            ++aCursor;
        }
    }
    return resultRange;
}

QuotaMode LayerMetasCreator::decideQuotaMode(const LayerKeyRange *range,
        size_t dimenCount, bool containUnSortedRange) const
{
    if (containUnSortedRange) {
        return QM_PER_LAYER;
    }
    LayerValidator validator;
    const shared_ptr<PartitionInfoWrapper> &partitionInfo = _readerPtr->getPartitionInfo();
    const PartitionMetaPtr &meta = partitionInfo->GetPartitionMeta();
    if (validator.validateRankHint(meta, _rankHint, range, dimenCount)) {
        return QM_PER_DOC;
    } else {
        return QM_PER_LAYER;
    }
}

} // namespace search
} // namespace isearch
