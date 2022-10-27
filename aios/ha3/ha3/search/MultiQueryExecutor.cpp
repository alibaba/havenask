#include <ha3/search/MultiQueryExecutor.h>
#include <assert.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MultiQueryExecutor);

MultiQueryExecutor::MultiQueryExecutor() { 
}

MultiQueryExecutor::~MultiQueryExecutor() { 
    for (QueryExecutorVector::iterator it = _queryExecutors.begin(); 
         it != _queryExecutors.end(); it++) 
    {
        POOL_DELETE_CLASS(*it);
    }
    _queryExecutors.clear();
}

const std::string MultiQueryExecutor::getName() const {
    return "MultiQueryExecutor";
}

// docid_t MultiQueryExecutor::getDocId() {
//     assert(false);
//     return INVALID_DOCID;
// }

/*
docid_t MultiQueryExecutor::seek(docid_t id) {
    assert(false);
    return INVALID_DOCID;
}
*/

void MultiQueryExecutor::moveToEnd() {
    QueryExecutor::moveToEnd();
    for (QueryExecutorVector::const_iterator it = _queryExecutors.begin(); 
         it != _queryExecutors.end(); it++) 
    {
        (*it)->moveToEnd();
    }
}

void MultiQueryExecutor::setCurrSub(docid_t docid) {
    QueryExecutor::setCurrSub(docid);
    for (auto it = _queryExecutors.begin(); it != _queryExecutors.end(); it++) {
        (*it)->setCurrSub(docid);
    }
}

uint32_t MultiQueryExecutor::getSeekDocCount() {
    uint32_t ret = 0;
    for (auto it = _queryExecutors.begin(); it != _queryExecutors.end(); it++) {
        ret += (*it)->getSeekDocCount();
    }
    return ret;
}

void MultiQueryExecutor::reset() {
    QueryExecutor::reset();
    for (QueryExecutorVector::const_iterator it = _queryExecutors.begin(); 
         it != _queryExecutors.end(); it++) 
    {
        (*it)->reset();
    }
}

std::string MultiQueryExecutor::toString() const {
    return convertToString(_queryExecutors);
}

END_HA3_NAMESPACE(search);

