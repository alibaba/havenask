#include <ha3/search/TermQueryExecutor.h>
#include <ha3/rank/MatchData.h>

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search)

HA3_LOG_SETUP(search, TermQueryExecutor);

TermQueryExecutor::TermQueryExecutor(PostingIterator *iter, 
                                     const Term &term)
    : QueryExecutor() 
    , _iter(iter)
    , _backupIter(NULL)
    , _term(term)
    , _indexPartReaderWrapper(NULL)
{
    if (_iter) {
        _mainChainDF = _iter->GetTermMeta()->GetDocFreq();
    }
    _hasSubDocExecutor = false;
}

TermQueryExecutor::~TermQueryExecutor() {
}

IE_NAMESPACE(common)::ErrorCode TermQueryExecutor::doSeek(docid_t id, docid_t& result)
{
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _iter->SeekDocWithErrorCode(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    ++_seekDocCount;
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE(common)::ErrorCode TermQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    assert(getDocId() >= docId);
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
    } else {
        result = END_DOCID;
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void TermQueryExecutor::getMetaInfo(rank::MetaInfo *metaInfo) const {
    rank::TermMetaInfo termMetaInfo;
    termMetaInfo.setMatchDataLevel(MDL_TERM);
    termMetaInfo.setQueryLabel(_queryLabel);
    termMetaInfo.setTerm(_term);
    if (_iter) {
        termMetaInfo.setTermMeta(_iter->GetTermMeta());
    } 
    HA3_LOG(TRACE3, "term_boost: %d", termMetaInfo.getTermBoost());
    metaInfo->pushBack(termMetaInfo);
}

df_t TermQueryExecutor::getDF(GetDFType type) const {
    if (!_iter) {
        return df_t(0);
    }
    if (type == GDT_MAIN_CHAIN) {
        return _iter->GetTermMeta()->GetDocFreq();
    } else {
        assert(type == GDT_CURRENT_CHAIN);
        return _iter->GetTruncateTermMeta()->GetDocFreq();
    }
}

void TermQueryExecutor::reset() {
    QueryExecutor::reset();
    assert(!_backupIter);
    _backupIter = _iter;
    if (_backupIter) {
        _iter = _backupIter->Clone();
        assert(_indexPartReaderWrapper != NULL);
        _indexPartReaderWrapper->addDelPosting(_iter);
    } else {
        _iter = NULL;
        moveToEnd();
    }
}

std::string TermQueryExecutor::toString() const {
    return _term.getWord().c_str();
}

END_HA3_NAMESPACE(search)
