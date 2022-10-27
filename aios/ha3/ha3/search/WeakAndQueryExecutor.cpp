#include <ha3/search/WeakAndQueryExecutor.h>
#include <algorithm>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, WeakAndQueryExecutor);

WeakAndQueryExecutor::WeakAndQueryExecutor(uint32_t minShouldMatch)
    : _count(0)
{
    _minShouldMatch = max(1u, minShouldMatch);
}

WeakAndQueryExecutor::~WeakAndQueryExecutor() {
}

void WeakAndQueryExecutor::doNthElement(std::vector<QueryExecutorEntry>& heap) {
    // sort minShoudMatch elements
    uint32_t i = 1;
    for (; i < _minShouldMatch; ++i) {
        if (heap[i].docId > heap[i+1].docId) {
            swap(heap[i], heap[i+1]);
        } else {
            break;
        }
    }
    if (i < _minShouldMatch) {
        return;
    }
    assert(i == _minShouldMatch);
    assert(_count >= _minShouldMatch);
    // heap adjustDown
    adjustDown(1, _count-_minShouldMatch+1, heap.data()+_minShouldMatch-1);
}

IE_NAMESPACE(common)::ErrorCode WeakAndQueryExecutor::doSeek(docid_t id, docid_t& result)
{
    docid_t docId = INVALID_DOCID;
    do {
        auto ec = getQueryExecutor(_sortNHeap[1].entryId)->seek(id, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _sortNHeap[1].docId = docId;
        doNthElement(_sortNHeap);
        if (id > _sortNHeap[1].docId) {
            continue;
        }
        id = _sortNHeap[_minShouldMatch].docId;
        if (_sortNHeap[1].docId == _sortNHeap[_minShouldMatch].docId) {
            break;
        }
    } while (END_DOCID != id);
    if (unlikely(END_DOCID == id)) {
        moveToEnd();
    }
    result = id;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void WeakAndQueryExecutor::reset() {
    MultiQueryExecutor::reset();
    addQueryExecutors(_queryExecutors);
}

IE_NAMESPACE(common)::ErrorCode WeakAndQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    if (!_hasSubDocExecutor) {
        docid_t currSubDocId = END_DOCID;
        if (getDocId() == docId && subDocId < subDocEnd) {
            currSubDocId = subDocId;
        }
        if (unlikely(needSubMatchdata)) {
            setCurrSub(currSubDocId);
        }
        result = currSubDocId;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    docid_t minSubDocId = END_DOCID;
    do {
        auto queryExecutor = getQueryExecutor(_sortNHeapSub[1].entryId);
        auto ec = queryExecutor->seekSub(docId, subDocId, END_DOCID, needSubMatchdata, minSubDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _sortNHeapSub[1].docId = minSubDocId;
        doNthElement(_sortNHeapSub);
        if (subDocId > _sortNHeapSub[1].docId) {
            continue;
        }
        subDocId = _sortNHeapSub[_minShouldMatch].docId;
        if (_sortNHeapSub[1].docId == _sortNHeapSub[_minShouldMatch].docId) {
            break;
        }
    } while (subDocId < subDocEnd);
    if (subDocId >= subDocEnd) {
        result = END_DOCID;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    result = subDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;    
}

void WeakAndQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    _hasSubDocExecutor = false;
    _queryExecutors = queryExecutors;
    _sortNHeap.clear();
    QueryExecutorEntry dumyEntry;
    _sortNHeap.push_back(dumyEntry);
    _sortNHeapSub.push_back(dumyEntry);
    _count = 0;
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        _hasSubDocExecutor = _hasSubDocExecutor || queryExecutors[i]->hasSubDocExecutor();
        QueryExecutorEntry newEntry;
        newEntry.docId = INVALID_DOCID;
        newEntry.entryId = _count ++;
        _sortNHeap.push_back(newEntry);
        _sortNHeapSub.push_back(newEntry);
    }
    assert(_count > 0);
    _minShouldMatch = min(_minShouldMatch, _count);
}

bool WeakAndQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        if(!(*it)->isMainDocHit(docId)) {
            return false;
        }
    }
    return true;
}

df_t WeakAndQueryExecutor::getDF(GetDFType type) const {
    df_t sumDF = 0;
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        sumDF += _queryExecutors[i]->getDF(type);
    }
    return sumDF;
}

string WeakAndQueryExecutor::toString() const {
    return autil::StringUtil::toString(_minShouldMatch) +
        "WEAKAND" + MultiQueryExecutor::toString();
}

END_HA3_NAMESPACE(search);
