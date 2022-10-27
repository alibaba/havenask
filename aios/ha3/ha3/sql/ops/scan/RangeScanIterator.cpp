#include <ha3/sql/ops/scan/RangeScanIterator.h>
#include <ha3/sql/common/common.h>

#include <vector>

BEGIN_HA3_NAMESPACE(sql);
RangeScanIterator::RangeScanIterator(const search::FilterWrapperPtr &filterWrapper,
                                     const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                                     const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
                                     const search::LayerMetaPtr &layerMeta,
                                     common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _filterWrapper(filterWrapper)
    , _delMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _rangeIdx(0)
    , _curId(-1)
{
}

bool RangeScanIterator::batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (batchSize == 0) {
        batchSize = DEFAULT_BATCH_COUNT;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    size_t minBatchSize = std::min(batchSize, (size_t)DEFAULT_BATCH_COUNT);
    docIds.reserve(minBatchSize);
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
            if (docIds.size() >= minBatchSize) {
                docCount += batchFilter(docIds, matchDocs);
                docIds.clear();
            }
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
        docCount += batchFilter(docIds, matchDocs);
    }
    return _isTimeout || _rangeIdx >= _layerMeta->size();
}

size_t RangeScanIterator::batchFilter(const std::vector<int32_t> &docIds, std::vector<matchdoc::MatchDoc> &matchDocs) {
    std::vector<matchdoc::MatchDoc> allocateDocs = _matchDocAllocator->batchAllocate(docIds);
    assert(docIds.size() == allocateDocs.size());
    size_t count = 0;
    if (_filterWrapper) {
        std::vector<matchdoc::MatchDoc> deallocateDocs;
        for (auto matchDoc : allocateDocs) {
            if (_filterWrapper->pass(matchDoc)) {
                ++count;
                matchDocs.push_back(matchDoc);
            } else {
                deallocateDocs.push_back(matchDoc);
            }
        }
        for (auto matchDoc : deallocateDocs) {
            _matchDocAllocator->deallocate(matchDoc);
        }
    } else {
        count = allocateDocs.size();
        matchDocs.insert(matchDocs.end(), allocateDocs.begin(), allocateDocs.end());
    }
    return count;
}


END_HA3_NAMESPACE(sql);

