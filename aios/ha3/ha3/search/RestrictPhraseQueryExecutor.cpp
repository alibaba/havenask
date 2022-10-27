#include <ha3/search/RestrictPhraseQueryExecutor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, RestrictPhraseQueryExecutor);

RestrictPhraseQueryExecutor::RestrictPhraseQueryExecutor(QueryExecutorRestrictor *restrictor) {
    _restrictor = restrictor;
}

RestrictPhraseQueryExecutor::~RestrictPhraseQueryExecutor() { 
    DELETE_AND_SET_NULL(_restrictor);
}

IE_NAMESPACE(common)::ErrorCode RestrictPhraseQueryExecutor::doSeek(docid_t id, docid_t& result) {
    if (unlikely(_hasSubDocExecutor)) {
        return AndQueryExecutor::doSeek(id, result);
    }
    docid_t tmpid = id;
    do {
        auto ec = AndQueryExecutor::doSeek(tmpid, tmpid);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpid == END_DOCID) {
            result = END_DOCID;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
        if (tmpid == _lastMatchedDocId) {
            result = tmpid;
            return IE_NAMESPACE(common)::ErrorCode::OK;            
        }
        docid_t restrictId = _restrictor->meetRestrict(tmpid);
        assert (restrictId >= tmpid);
        if (restrictId == END_DOCID) {
            result = END_DOCID;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        } 
        if (restrictId == tmpid) {
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_NAMESPACE(common)::ErrorCode::OK;
            } else {
                tmpid++;
            }
        } else {
            tmpid = restrictId;
        }
    } while(true);
    
    result = END_DOCID;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}




END_HA3_NAMESPACE(search);

