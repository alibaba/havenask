#include <ha3/sql/ops/scan/QueryScanIterator.h>
#include <ha3/sql/common/common.h>
#include <ha3/search/TermQueryExecutor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(scan, QueryScanIterator);

QueryScanIterator::QueryScanIterator(const search::QueryExecutorPtr &queryExecutor,
                                     const search::FilterWrapperPtr &filterWrapper,
                                     const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                                     const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
                                     const search::LayerMetaPtr &layerMeta,
                                     common::TimeoutTerminator *timeoutTerminator)
    : ScanIterator(matchDocAllocator, timeoutTerminator)
    , _queryExecutor(queryExecutor)
    , _filterWrapper(filterWrapper)
    , _deletionMapReader(delMapReader)
    , _layerMeta(layerMeta)
    , _postingIter(NULL)
    , _curDocId((*layerMeta)[0].nextBegin)
    , _curBegin((*layerMeta)[0].nextBegin)
    , _curEnd((*layerMeta)[0].end)
    , _rangeCousor(0)
    , _batchFilterTime(0)
    , _scanTime(0)
{
    search::TermQueryExecutor* termExecutor =
        dynamic_cast<search::TermQueryExecutor*>(_queryExecutor.get());
    if (termExecutor != NULL) {
        _postingIter = termExecutor->getPostingIterator();
    }
}

QueryScanIterator::~QueryScanIterator() {
    _postingIter = NULL;
}

bool QueryScanIterator::batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (_curDocId == END_DOCID) {
        return true;
    }
    constexpr size_t DEFAULT_SCAN_BATCH = 1024;
    int64_t beginTime = autil::TimeUtility::currentTime();
    if (batchSize == 0) {
        batchSize = DEFAULT_SCAN_BATCH;
    }
    size_t docCount = 0;
    std::vector<int32_t> docIds;
    size_t minBatchSize = std::min(batchSize, (size_t)DEFAULT_SCAN_BATCH);
    docIds.reserve(minBatchSize);
    docid_t docId = _curDocId;
    assert(docId >= _curBegin);
    if (_postingIter) {
        while (true) {
            if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
                _isTimeout = true;
                break;
            }
            docId = _postingIter->SeekDoc(docId);
            if (docId == INVALID_DOCID) {
                docId = END_DOCID;
                break;
            }
            if (unlikely(!tryToMakeItInRange(docId))) {
                if (docId == END_DOCID) {
                    break;
                }
                continue;
            }
            ++_totalScanCount;
            if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
                ++docId;
                continue;
            }
            docIds.push_back(docId);
            ++docId;
            if (docIds.size() >= minBatchSize) {
                docCount += batchFilter(docIds, matchDocs);
                docIds.clear();
                if (docCount >= batchSize) {
                    break;
                }
            }
        }
    } else {
        IE_NAMESPACE(common)::ErrorCode ec;
        while (true) {
            if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
                _isTimeout = true;
                break;
            }
            ec = _queryExecutor->seekWithoutCheck(docId, docId);
            if (ec != IE_NAMESPACE(common)::ErrorCode::OK) {
                SQL_LOG(DEBUG, "query executor has error.");
                break;
            }
            if (unlikely(!tryToMakeItInRange(docId))) {
                if (docId == END_DOCID) {
                    break;
                }
                continue;
            }
            _totalScanCount++;
            if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
                ++docId;
                continue;
            }
            docIds.push_back(docId);
            ++docId;
            if (docIds.size() >= minBatchSize) {
                docCount += batchFilter(docIds, matchDocs);
                docIds.clear();
                if (docCount >= batchSize) {
                    break;
                }
            }
        }
    }
    if (docIds.size() > 0) {
        docCount += batchFilter(docIds, matchDocs);
    }
    _curDocId = docId;
    int64_t endTime = autil::TimeUtility::currentTime();
    _scanTime += endTime - beginTime;
    return _isTimeout || _curDocId == END_DOCID;
}

size_t QueryScanIterator::batchFilter(const std::vector<int32_t> &docIds, std::vector<matchdoc::MatchDoc> &matchDocs) {
    int64_t beginTime = autil::TimeUtility::currentTime();
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
        _matchDocAllocator->deallocate(deallocateDocs.data(), deallocateDocs.size());
    } else {
        count = allocateDocs.size();
        matchDocs.insert(matchDocs.end(), allocateDocs.begin(), allocateDocs.end());
    }
    int64_t endTime = autil::TimeUtility::currentTime();
    _batchFilterTime += endTime - beginTime;
    return count;
}

bool QueryScanIterator::moveToCorrectRange(docid_t &docId) {
    while (++_rangeCousor < _layerMeta->size()) {
        if (docId <= (*_layerMeta)[_rangeCousor].end) {
            _curBegin = (*_layerMeta)[_rangeCousor].begin;
            _curEnd = (*_layerMeta)[_rangeCousor].end;
            if (docId < _curBegin) {
                docId = _curBegin;
                return false;
            } else {
                return true;
            }
        }
    }
    docId = END_DOCID;
    return false;
}

END_HA3_NAMESPACE(sql);
