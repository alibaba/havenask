#include "indexlib/merger/document_reclaimer/term_posting_executor.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, TermPostingExecutor);

TermPostingExecutor::TermPostingExecutor(const PostingIteratorPtr& postingIterator)
    : mIter(postingIterator)
{
}

TermPostingExecutor::~TermPostingExecutor() 
{
}

df_t TermPostingExecutor::GetDF() const
{
    return mIter->GetTermMeta()->GetDocFreq();
}

docid_t TermPostingExecutor::DoSeek(docid_t id)
{
    docid_t docId = mIter->SeekDoc(id);
    return (docId == INVALID_DOCID) ? END_DOCID : docId;
}


IE_NAMESPACE_END(merger);

