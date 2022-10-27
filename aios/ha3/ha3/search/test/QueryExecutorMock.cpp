#include <ha3/search/test/QueryExecutorMock.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, QueryExecutorMock);

QueryExecutorMock::QueryExecutorMock(const std::string &docIdStr)
    : _cursor(0)
{
    autil::StringTokenizer st(docIdStr, ",", autil::StringTokenizer::TOKEN_IGNORE_EMPTY
                              | autil::StringTokenizer::TOKEN_TRIM);
    _docIds.reserve(st.getNumTokens());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        autil::StringTokenizer st1(st[i], "-", autil::StringTokenizer::TOKEN_IGNORE_EMPTY
                              | autil::StringTokenizer::TOKEN_TRIM);
        if (st1.getNumTokens() == 1) {
            _docIds.push_back(autil::StringUtil::fromString<docid_t>(st1[0].c_str()));
        } else {
            assert(st1.getNumTokens() == 2);
            docid_t begin = autil::StringUtil::fromString<docid_t>(st1[0].c_str());
            docid_t end = autil::StringUtil::fromString<docid_t>(st1[1].c_str());
            for (docid_t docId = begin; docId <= end; ++docId) {
                _docIds.push_back(docId);
            }
        }
    }
}

QueryExecutorMock::QueryExecutorMock(const std::vector<docid_t> &docIds)
    : _cursor(0)
    , _docIds(docIds)
{
}

QueryExecutorMock::~QueryExecutorMock() { 
}

IE_NAMESPACE(common)::ErrorCode QueryExecutorMock::doSeek(docid_t id, docid_t& result) {
    while (true) {
        if (_cursor >= _docIds.size()) {
            result = END_DOCID;
            return IE_OK;
        }
        if (_docIds[_cursor] < id) {
            ++_cursor;
            continue;
        }
        break;
    }
    result = _docIds[_cursor++];
    return IE_OK;
}

void QueryExecutorMock::reset() {
    QueryExecutor::reset();
    _cursor = 0;
}

std::string QueryExecutorMock::toString() const {
    return "";
}

docid_t QueryExecutorMock::seekSubDoc(docid_t docId, docid_t subDocId,
                                      docid_t subDocEnd, bool needSubMatchdata)
{
    assert(getDocId() <= docId);
    if (getDocId() == docId && subDocId < subDocEnd) {
        return subDocId;
    } 
    return END_DOCID;
}

END_HA3_NAMESPACE(search);

