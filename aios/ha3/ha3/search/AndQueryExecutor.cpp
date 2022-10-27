#include <ha3/search/AndQueryExecutor.h>
#include <iostream>
using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AndQueryExecutor);

AndQueryExecutor::AndQueryExecutor()
    : _testDocCount(0)
{
    _firstExecutor = NULL;
}

AndQueryExecutor::~AndQueryExecutor() {
}

const std::string AndQueryExecutor::getName() const {
    return "AndQueryExecutor";
}

void AndQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    _hasSubDocExecutor = false;
    if (unlikely(queryExecutors.empty())) {
        return;
    }

    for(size_t i = 0; i < queryExecutors.size(); i++) {
        _hasSubDocExecutor = _hasSubDocExecutor || queryExecutors[i]->hasSubDocExecutor();
    }
    _queryExecutors = queryExecutors;
    //_sortedQueryExecutors = queryExecutors;
    QueryExecutorVector tmpQueryExecutors = queryExecutors;
    sort(tmpQueryExecutors.begin(), tmpQueryExecutors.end(), DFCompare());
    _sortedQueryExecutors.push_back(tmpQueryExecutors[0]);
    for (size_t i = 1; i < tmpQueryExecutors.size(); i++) {
        auto filter = tmpQueryExecutors[i]->stealFilter();
        if (filter) {
            _filters.push_back(filter);
        } else {
            _sortedQueryExecutors.push_back(tmpQueryExecutors[i]);
        }
    }
    _firstExecutor = &_sortedQueryExecutors[0];
}

IE_NAMESPACE(common)::ErrorCode AndQueryExecutor::doSeek(docid_t id, docid_t& result) {
    QueryExecutor **firstExecutor = _firstExecutor;
    QueryExecutor **currentExecutor = firstExecutor;
    QueryExecutor **endExecutor = firstExecutor + _sortedQueryExecutors.size();
    docid_t current = id;
    do {
        docid_t tmpid = INVALID_DOCID;
        auto ec = (*currentExecutor)->seek(current, tmpid);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpid == END_DOCID) {
            current = tmpid;
            break;
        } else if (tmpid != current) {
            current = tmpid;
            currentExecutor = firstExecutor;
        } else {
            currentExecutor++;
            if (currentExecutor >= endExecutor) {
                if (testCurrentDoc(current)) {
                    result = current;
                    return IE_NAMESPACE(common)::ErrorCode::OK;
                } else {
                    current++;
                    currentExecutor = firstExecutor;
                }
            }
        }
    } while (true);
    // move all child executor to end, make sure all child node do not unpack.
    assert(current == END_DOCID);
    moveToEnd();
    result = current;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode AndQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
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
    QueryExecutor **firstExecutor = _firstExecutor;
    QueryExecutor **currentExecutor = firstExecutor;
    QueryExecutor **endExecutor = firstExecutor + _sortedQueryExecutors.size();
    docid_t current = subDocId;
    do {
        docid_t tmpid = INVALID_DOCID;
        auto ec = (*currentExecutor)->seekSub(docId, current, subDocEnd, needSubMatchdata, tmpid);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpid != current) {
            current = tmpid;
            currentExecutor = firstExecutor;
        } else {
            currentExecutor++;
        }

        if (current >= subDocEnd) {
            break;
        }

        if (currentExecutor >= endExecutor) {
            //test main doc
            if (testCurrentDoc(docId)) {
                break;
            } else {
                current = END_DOCID;
                currentExecutor = firstExecutor;
                break;
            }
        }
    } while (true);
    result = current;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

bool AndQueryExecutor::isMainDocHit(docid_t docId) const {
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

df_t AndQueryExecutor::getDF(GetDFType type) const {
    df_t minDF = numeric_limits<df_t>::max();
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        minDF = min(minDF, _queryExecutors[i]->getDF(type));
    }
    return minDF;
}

string AndQueryExecutor::toString() const {
    return "AND" + 
        MultiQueryExecutor::convertToString(_sortedQueryExecutors);
}

END_HA3_NAMESPACE(search);

