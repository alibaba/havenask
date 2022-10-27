#include <ha3/search/BufferedTermQueryExecutor.h>

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, BufferedTermQueryExecutor);

BufferedTermQueryExecutor::BufferedTermQueryExecutor(PostingIterator *iter, 
        const Term &term)
    : TermQueryExecutor(iter, term)
{ 
    initBufferedPostingIterator();
}

BufferedTermQueryExecutor::~BufferedTermQueryExecutor() { 
}

IE_NAMESPACE(common)::ErrorCode BufferedTermQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t tempDocId = INVALID_DOCID;
    auto ec = _bufferedIter->InnerSeekDoc(id, tempDocId);
    IE_RETURN_CODE_IF_ERROR(ec);
    ++_seekDocCount;
    if (tempDocId == INVALID_DOCID) {
        tempDocId = END_DOCID;
    }
    result = tempDocId;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

void BufferedTermQueryExecutor::initBufferedPostingIterator() {
    _bufferedIter = dynamic_cast<BufferedPostingIterator*>(_iter);
}

END_HA3_NAMESPACE(search);

