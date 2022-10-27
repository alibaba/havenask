#ifndef __INDEXLIB_TERM_POSTING_EXECUTOR_H
#define __INDEXLIB_TERM_POSTING_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/document_reclaimer/posting_executor.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

IE_NAMESPACE_BEGIN(merger);

class TermPostingExecutor : public PostingExecutor
{
public:
    TermPostingExecutor(const index::PostingIteratorPtr& postingIterator);
    ~TermPostingExecutor();
public:
    df_t GetDF() const override;
    docid_t DoSeek(docid_t id) override;

private:
    index::PostingIteratorPtr mIter;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TermPostingExecutor);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_TERM_POSTING_EXECUTOR_H
