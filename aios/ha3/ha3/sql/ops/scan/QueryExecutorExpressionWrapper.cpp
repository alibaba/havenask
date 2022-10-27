#include <ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);

QueryExecutorExpressionWrapper::QueryExecutorExpressionWrapper(search::QueryExecutor* queryExecutor)
    : _queryExecutor(queryExecutor)
{}

QueryExecutorExpressionWrapper::~QueryExecutorExpressionWrapper() {
    if (_queryExecutor) {
        POOL_DELETE_CLASS(_queryExecutor);
        _queryExecutor = NULL;
    }
}

bool QueryExecutorExpressionWrapper::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    if (!_queryExecutor) {
        return false;
    }
    docid_t docId = matchDoc.getDocId();
    docid_t tmpDocId;
    IE_NAMESPACE(common)::ErrorCode ec = _queryExecutor->seek(docId, tmpDocId);
    if (ec != IE_NAMESPACE(common)::ErrorCode::OK) {
        return false;
    }
    return  tmpDocId == docId ;
}

END_HA3_NAMESPACE(sql);
