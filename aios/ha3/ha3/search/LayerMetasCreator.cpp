#include <ha3/search/LayerMetasCreator.h>
#include <stdlib.h>
#include <ha3/search/LayerMetaUtil.h>
#include <ha3/search/LayerValidator.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, LayerMetasCreator);

IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
USE_HA3_NAMESPACE(common);
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
        return createDefaultLayerMetas();
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
        return createDefaultLayerMetas();
    }
    return layerMetas;
}

LayerMetas* LayerMetasCreator::createDefaultLayerMetas() const {
    LayerMetas *layerMetas = POOL_NEW_CLASS(_pool, LayerMetas, _pool);
    LayerMeta layerMeta = createFullRange();
    layerMeta.quota = numeric_limits<uint32_t>::max();
    layerMetas->push_back(layerMeta);
    return layerMetas;
}

// each layer range key create a new range based on layerMeta.
// layerMeta is full range in the beginning.
// sorted range must be continuous for indexlib api.
// set other metas(like quota,quotaMode etc.) in the end.
LayerMeta LayerMetasCreator::createLayerMeta(const LayerDescription *desc,
        const LayerMeta &visitedRange) const
{
    LayerMeta layerMeta = createFullRange();
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

DimensionDescriptionVector LayerMetasCreator::convert2DimenDescription(
        const LayerDescription* layerDesc,
        int32_t from, int32_t to)
{
    DimensionDescriptionVector dimens;
    for (int32_t i = from; i <= to; ++i) {
        LayerKeyRange* keyRange = layerDesc->getLayerRange(i);
        DimensionDescriptionPtr dimen(new DimensionDescription);
        dimen->name = keyRange->attrName;
        dimen->values = keyRange->values;
        for (size_t j = 0; j < keyRange->ranges.size(); ++j) {
            dimen->ranges.push_back(
                    RangeDescription(keyRange->ranges[j].from,
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
    DimensionDescriptionVector dimens = convert2DimenDescription(layerDesc, from, to);
    for (size_t i = 0; i < lastRange.size(); ++i) {
        DocIdRange rangeLimit(lastRange[i].begin, lastRange[i].end + 1);
        if (rangeLimit.second < rangeLimit.first) {
            continue;
        }
        DocIdRangeVector resultRanges;
        if (!_readerPtr->getReader()->GetSortedDocIdRanges(
                        dimens, rangeLimit, resultRanges))
        {
            // validate failed, make quotaMode per layer to make sure seek all doc
            layerMeta = lastRange;
            layerMeta.quotaMode = QM_PER_LAYER;
            return layerMeta;
        }
        for (size_t j = 0; j < resultRanges.size(); ++j) {
            DocIdRange range(resultRanges[j].first, 
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
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
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
    DocIdRange unSortedRange;
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
    partitionInfo->GetUnorderedDocIdRange(unSortedRange);
    --unSortedRange.second;
    if (unSortedRange.second >= unSortedRange.first) {
        layerMeta.push_back(unSortedRange);
    }
    return layerMeta;
}

LayerMeta LayerMetasCreator::orderedDocIdConvert() const {
    LayerMeta layerMeta(_pool);
    DocIdRangeVector ranges;
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
    partitionInfo->GetOrderedDocIdRanges(ranges);
    for (uint32_t i = 0; i < ranges.size(); ++i) {
        DocIdRangeMeta rangeMeta(ranges[i].first, ranges[i].second - 1);
        layerMeta.push_back(rangeMeta);
    }
    return layerMeta;
}

LayerMeta LayerMetasCreator::docidLayerConvert(const LayerKeyRange *layerRange) const {
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
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
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
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

LayerMeta LayerMetasCreator::createFullRange() const {
    const PartitionInfoPtr &partInfo = _readerPtr->getPartitionInfo();
    LayerMeta layerMeta(_pool);

    DocIdRangeVector ranges;
    if (partInfo->GetOrderedDocIdRanges(ranges)) {
        for (size_t i = 0; i < ranges.size(); ++i) {
            DocIdRange& range = ranges[i];
            --range.second;
            if (range.second >= range.first) {
                layerMeta.push_back(range);
            }
        }
    }
    DocIdRange unSortedRange;
    if (partInfo->GetUnorderedDocIdRange(unSortedRange)) {
        --unSortedRange.second;
        if (unSortedRange.second >= unSortedRange.first) {
            layerMeta.push_back(unSortedRange);
        }
    }
    return layerMeta;
}

QuotaMode LayerMetasCreator::decideQuotaMode(const LayerKeyRange *range,
        size_t dimenCount, bool containUnSortedRange) const
{
    if (containUnSortedRange) {
        return QM_PER_LAYER;
    }
    LayerValidator validator;
    const PartitionInfoPtr &partitionInfo = _readerPtr->getPartitionInfo();
    const PartitionMetaPtr &meta = partitionInfo->GetPartitionMeta();
    if (validator.validateRankHint(meta, _rankHint, range, dimenCount)) {
        return QM_PER_DOC;
    } else {
        return QM_PER_LAYER;
    }
}

END_HA3_NAMESPACE(search);

