#include <ha3/search/OrQueryExecutor.h>

using namespace std;
USE_HA3_NAMESPACE(rank);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, OrQueryExecutor);

OrQueryExecutor::OrQueryExecutor() {
    _count = 0;    
}

OrQueryExecutor::~OrQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode OrQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t docId = INVALID_DOCID;
    do {
        auto ec = getQueryExecutor(_qeMinHeap[1].entryId)->seek(id, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _qeMinHeap[1].docId = docId;
        adjustDown(1, _count, _qeMinHeap.data());
    } while (id > _qeMinHeap[1].docId);
    result =  _qeMinHeap[1].docId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void OrQueryExecutor::reset() {
    MultiQueryExecutor::reset();
    addQueryExecutors(_queryExecutors);
}

IE_NAMESPACE(common)::ErrorCode OrQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
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
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    docid_t minSubDocId = END_DOCID;
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        docid_t ret = INVALID_DOCID;
        auto ec = (*it)->seekSub(docId, subDocId, subDocEnd, needSubMatchdata, ret);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (ret < minSubDocId) {
            minSubDocId = ret;
        }
    }
    result = minSubDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void OrQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    _hasSubDocExecutor = false;
    _queryExecutors = queryExecutors;
    _qeMinHeap.clear();
    QueryExecutorEntry dumyEntry;
    _qeMinHeap.push_back(dumyEntry);
    _count = 0;
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        _hasSubDocExecutor = _hasSubDocExecutor || queryExecutors[i]->hasSubDocExecutor();
        QueryExecutorEntry newEntry;
        newEntry.docId = INVALID_DOCID;
        newEntry.entryId = _count ++;
        _qeMinHeap.push_back(newEntry);
    }
}

bool OrQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        if((*it)->getDocId() == docId && (*it)->isMainDocHit(docId)) {
            return true;
        }
    }
    return false;
}


df_t OrQueryExecutor::getDF(GetDFType type) const {
    df_t sumDF = 0;
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        sumDF += _queryExecutors[i]->getDF(type);
    }
    return sumDF;
}

string OrQueryExecutor::toString() const {
    return "OR" + MultiQueryExecutor::toString();
}

END_HA3_NAMESPACE(search);

