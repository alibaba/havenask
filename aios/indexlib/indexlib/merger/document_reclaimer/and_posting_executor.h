#ifndef __INDEXLIB_AND_POSTING_OPERATOR_H
#define __INDEXLIB_AND_POSTING_OPERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/document_reclaimer/posting_executor.h"

IE_NAMESPACE_BEGIN(merger);

class AndPostingExecutor : public PostingExecutor
{
public:
    AndPostingExecutor(
        const std::vector<PostingExecutorPtr>& postingExecutors);
    ~AndPostingExecutor();

public:
    struct DFCompare
    {
        bool operator () (const PostingExecutorPtr& lhs, const PostingExecutorPtr& rhs)
        {
            return lhs->GetDF() < rhs->GetDF();
        }
    };
    df_t GetDF() const override;
    docid_t DoSeek(docid_t docId) override;

private:
    std::vector<PostingExecutorPtr> mPostingExecutors;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AndPostingExecutor);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_AND_POSTING_OPERATOR_H
