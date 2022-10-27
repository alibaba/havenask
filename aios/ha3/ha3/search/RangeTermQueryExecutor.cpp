#include <ha3/search/RangeTermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h>

using namespace std;
IE_NAMESPACE_USE(index);


BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, RangeTermQueryExecutor);

RangeTermQueryExecutor::RangeTermQueryExecutor(
        IE_NAMESPACE(index)::PostingIterator *iter, 
        const common::Term &term)
    : TermQueryExecutor(iter, term)
    , _filter(NULL)
{
    initRangePostingIterator();
}

RangeTermQueryExecutor::~RangeTermQueryExecutor() {
}

IE_NAMESPACE(common)::ErrorCode RangeTermQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _rangeIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void RangeTermQueryExecutor::initRangePostingIterator() {
    SeekAndFilterIterator* compositeIter = (SeekAndFilterIterator*)_iter;
    _rangeIter =
        (IE_NAMESPACE(index)::RangePostingIterator*)compositeIter->GetIndexIterator();
    _filter = compositeIter->GetDocValueFilter();
}

END_HA3_NAMESPACE(search);

