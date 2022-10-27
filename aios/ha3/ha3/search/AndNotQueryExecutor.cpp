#include <ha3/search/AndNotQueryExecutor.h>

using namespace std;
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AndNotQueryExecutor);

AndNotQueryExecutor::AndNotQueryExecutor() 
    : _leftQueryExecutor(NULL)
    , _rightQueryExecutor(NULL)
    , _rightFilter(NULL)
    , _testDocCount(0)
    , _rightExecutorHasSub(false)
{
}

AndNotQueryExecutor::~AndNotQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode AndNotQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t docId = INVALID_DOCID;
    auto ec = _leftQueryExecutor->seek(id, docId);
    IE_RETURN_CODE_IF_ERROR(ec);
    docid_t tmpId = INVALID_DOCID;
    while (_rightQueryExecutor && docId != END_DOCID) { 
        if (_rightFilter) {
            _testDocCount++;
            if (_rightFilter->Test(docId)) {
                tmpId = docId;
            } else {
                tmpId = END_DOCID;
            }
        } else {
            auto ec = _rightQueryExecutor->seek(docId, tmpId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }

        if (_rightExecutorHasSub) {
            break;
        } else if (docId == tmpId) {
            auto ec = _leftQueryExecutor->seek(docId + 1, docId);
            IE_RETURN_CODE_IF_ERROR(ec);
        } else {
            break;
        }
    }
    result = docId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode AndNotQueryExecutor::seekSubDoc(docid_t id, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    // right query must not contain current doc for now.
    do {
        auto ec = _leftQueryExecutor->seekSub(id, subDocId, subDocEnd,
                needSubMatchdata, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId >= subDocEnd) {
            result = END_DOCID;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
        docid_t rightDocId = INVALID_DOCID;
        ec = _rightQueryExecutor->seekSub(id, subDocId, subDocEnd,
                needSubMatchdata, rightDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (rightDocId == subDocId) {
            ec = _leftQueryExecutor->seekSub(id, subDocId + 1, subDocEnd,
                    needSubMatchdata, subDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        } else {
            break;
        }
    } while (subDocId < subDocEnd);
    result = subDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void AndNotQueryExecutor::addQueryExecutors(
        QueryExecutor* leftExecutor,
        QueryExecutor* rightExecutor)
{
     
    _leftQueryExecutor = leftExecutor;
    _hasSubDocExecutor = leftExecutor->hasSubDocExecutor();
    _rightQueryExecutor = rightExecutor;
    _rightExecutorHasSub = rightExecutor->hasSubDocExecutor();
    if (_rightQueryExecutor && _leftQueryExecutor &&
        _rightQueryExecutor->getCurrentDF() >
        _leftQueryExecutor->getCurrentDF())
    {
        _rightFilter = rightExecutor->stealFilter();
    }
    // add all query executor to multi query executor for deconstruct,unpack,getMetaInfo.
    _queryExecutors.push_back(_leftQueryExecutor);
    if (_rightQueryExecutor) {
        _hasSubDocExecutor = _hasSubDocExecutor || _rightQueryExecutor->hasSubDocExecutor();
        _queryExecutors.push_back(_rightQueryExecutor);
    }
    if (_rightQueryExecutor) {
        _rightExecutorHasSub = _rightQueryExecutor->hasSubDocExecutor();
    }
}

bool AndNotQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    if (_rightExecutorHasSub) {
        return false;
    }
    return _leftQueryExecutor->isMainDocHit(docId);
}

df_t AndNotQueryExecutor::getDF(GetDFType type) const {
    return _queryExecutors[0]->getDF(type);
}

string AndNotQueryExecutor::toString() const {
    string ret = "ANDNOT(" + _leftQueryExecutor->toString();
    if (_rightQueryExecutor) {
        ret += "," + _rightQueryExecutor->toString();
    }
    ret += ")";
    return ret;
}

void AndNotQueryExecutor::setCurrSub(docid_t docid) {
    QueryExecutor::setCurrSub(docid);
    _leftQueryExecutor->setCurrSub(docid);
}

END_HA3_NAMESPACE(search);

