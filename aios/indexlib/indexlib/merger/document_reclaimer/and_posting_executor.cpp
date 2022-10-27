#include "indexlib/merger/document_reclaimer/and_posting_executor.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, AndPostingExecutor);

AndPostingExecutor::AndPostingExecutor(
    const std::vector<PostingExecutorPtr>& postingExecutors)
    : mPostingExecutors(postingExecutors)
{
    sort(mPostingExecutors.begin(), mPostingExecutors.end(), DFCompare());
}

AndPostingExecutor::~AndPostingExecutor() 
{
}

df_t AndPostingExecutor::GetDF() const
{
    df_t minDF = numeric_limits<df_t>::max();
    for (size_t i = 0; i < mPostingExecutors.size(); ++i)
    {
        minDF = min(minDF, mPostingExecutors[i]->GetDF());
    }
    return minDF;
}

docid_t AndPostingExecutor::DoSeek(docid_t id)
{
    auto firstIter = mPostingExecutors.begin();
    auto currentIter = firstIter;
    auto endIter = mPostingExecutors.end();

    docid_t current = id;
    do {
        docid_t tmpId = (*currentIter)->Seek(current);
        if (tmpId != current)
        {
            current = tmpId;
            currentIter = firstIter;
        } 
        else
        {
            ++currentIter;
        }
    } 
    while (END_DOCID != current && currentIter != endIter);
    return current;
}

IE_NAMESPACE_END(merger);

