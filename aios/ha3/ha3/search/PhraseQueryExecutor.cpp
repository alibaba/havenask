#include <ha3/search/PhraseQueryExecutor.h>
#include <ha3/common.h>
#include <limits.h>
#include <assert.h>
#include <vector>

using namespace std;
USE_HA3_NAMESPACE(index);
BEGIN_HA3_NAMESPACE(search)

HA3_LOG_SETUP(search, PhraseQueryExecutor);
PhraseQueryExecutor::PhraseQueryExecutor()
    : _lastMatchedDocId(INVALID_DOCID)
{
}

PhraseQueryExecutor::~PhraseQueryExecutor() {
}

void PhraseQueryExecutor::addTermQueryExecutors(const vector<TermQueryExecutor*> &termQueryExecutors) {
    _termExecutors = termQueryExecutors;
    vector<QueryExecutor*> queryExecutors(termQueryExecutors.begin(), termQueryExecutors.end());
    AndQueryExecutor::addQueryExecutors(queryExecutors);
}

IE_NAMESPACE(common)::ErrorCode PhraseQueryExecutor::doSeek(docid_t id, docid_t& result) {
    if (unlikely(_hasSubDocExecutor)) {
        return AndQueryExecutor::doSeek(id, result);
    }
    docid_t tmpid = id;
    docid_t tmpResult = INVALID_DOCID;
    do {
        auto ec = AndQueryExecutor::doSeek(tmpid, tmpResult);
        IE_RETURN_CODE_IF_ERROR(ec);
        tmpid = tmpResult;
        if (tmpid != END_DOCID) {
            if (_lastMatchedDocId) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            tmpid++;
        } else {
            result = END_DOCID;
            return IE_NAMESPACE(common)::ErrorCode::OK;            
        }
    } while(true);

    result = END_DOCID;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode PhraseQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId, 
                           docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    if (likely(!_hasSubDocExecutor)) {
        return AndQueryExecutor::seekSubDoc(docId, subDocId, subDocEnd, needSubMatchdata, result);
    }
    docid_t tmpid = subDocId;
    docid_t tmpResult = INVALID_DOCID;
    do {
        auto ec = AndQueryExecutor::seekSubDoc(docId, tmpid, subDocEnd, needSubMatchdata, tmpResult);
        IE_RETURN_CODE_IF_ERROR(ec);
        tmpid = tmpResult;
        if (tmpid != END_DOCID) {
            if (_lastMatchedDocId == tmpid) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;
            }
            bool isPhraseFreq = false;
            ec = phraseFreq(isPhraseFreq);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (isPhraseFreq) {
                _lastMatchedDocId = tmpid;
                result = tmpid;
                return IE_OK;                
            }
            tmpid++;
        } else {
            result = END_DOCID;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
    } while(true);
    
    result = END_DOCID;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void PhraseQueryExecutor::addRelativePostion(int32_t termPos, int32_t postingMark)
{
    _termReleativePos.push_back(termPos);
    _postingsMark.push_back(postingMark);
}


string PhraseQueryExecutor::toString() const {
    return "PHRASE" + MultiQueryExecutor::toString();
}


END_HA3_NAMESPACE(search)
