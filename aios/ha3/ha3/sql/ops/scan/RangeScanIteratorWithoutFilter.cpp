#include <ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h>

BEGIN_HA3_NAMESPACE(sql);

RangeScanIteratorWithoutFilter::RangeScanIteratorWithoutFilter(
        const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
        const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
        const search::LayerMetaPtr &layerMeta,
        common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _delMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _rangeIdx(0)
    , _curId(-1)
{
}

bool RangeScanIteratorWithoutFilter::batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (batchSize == 0) {
        batchSize = DEFAULT_BATCH_COUNT;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    docIds.reserve(batchSize);
    for (; _rangeIdx < _layerMeta->size(); ++_rangeIdx) {
        auto &range = (*_layerMeta)[_rangeIdx];
        _curId = _curId >= (range.begin - 1) ? _curId : (range.begin - 1);
        while (docCount < batchSize && ++_curId <= range.end ) {
            if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
                _isTimeout = true;
                break;
            }
            ++_totalScanCount;
            if (_delMapReader && _delMapReader->IsDeleted(_curId)) {
                continue;
            }
            docIds.push_back(_curId);
            ++docCount;
        }
        if (docCount >= batchSize) {
            break;
        }
        --_curId;
        if (_isTimeout) {
            break;
        }
    }
    if (docIds.size() > 0) {
        matchDocs = _matchDocAllocator->batchAllocate(docIds);
    }
    return _isTimeout || _rangeIdx >= _layerMeta->size();
}



END_HA3_NAMESPACE(sql);

