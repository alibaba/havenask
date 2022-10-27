#include <ha3/search/SubTermQueryExecutor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SubTermQueryExecutor);

SubTermQueryExecutor::SubTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
        const common::Term &term,
        DocMapAttrIterator *mainToSubIter,
        DocMapAttrIterator *subToMainIter)
    : TermQueryExecutor(iter, term)
    , _curSubDocId(INVALID_DOCID)
    , _mainToSubIter(mainToSubIter)
    , _subToMainIter(subToMainIter)
{
    _hasSubDocExecutor = true;
}

SubTermQueryExecutor::~SubTermQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode SubTermQueryExecutor::doSeek(docid_t docId, docid_t& result) {
    docid_t subDocStartId = 0;
    if (likely(docId != 0)) {
        bool ret = _mainToSubIter->Seek(docId - 1, subDocStartId);
        assert(ret); (void)ret;
    }
    if (unlikely(INVALID_DOCID == subDocStartId)) {
        // building doc in rt: set sub join, but not set main join
        result = END_DOCID;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    docid_t retDocId = INVALID_DOCID;
    docid_t subDocId = subDocStartId;
    do {
        auto ec = subDocSeek(subDocId, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId == END_DOCID) {
            break;
        } 
        bool ret = _subToMainIter->Seek(subDocId, retDocId);
        assert(ret); (void)ret;
        subDocId++;
    } while (retDocId == INVALID_DOCID);
   
    if (unlikely(retDocId == INVALID_DOCID)) {
        retDocId = END_DOCID;
    }
    if (unlikely(retDocId < docId)) {
        HA3_LOG(ERROR, "Invalid map between sub and main table."
                "main doc[%d] map to start sub doc[%d], "
                "seek sub doc[%d] map to main doc[%d].",
                docId, subDocStartId, subDocId, retDocId);
        retDocId = END_DOCID;
    }
    assert(retDocId >= docId);
    result = retDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode SubTermQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    auto ec = subDocSeek(subDocId, subDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (subDocId < subDocEnd) {
        result = subDocId;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
    result = END_DOCID;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

bool SubTermQueryExecutor::isMainDocHit(docid_t docId) const {
    return false;
}

void SubTermQueryExecutor::reset() {
    TermQueryExecutor::reset();
    _curSubDocId = INVALID_DOCID;
}

string SubTermQueryExecutor::toString() const {
    return "sub_term:" + TermQueryExecutor::toString();
}

END_HA3_NAMESPACE(search);

