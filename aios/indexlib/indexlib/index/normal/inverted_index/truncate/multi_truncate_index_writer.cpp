#include "indexlib/index/normal/inverted_index/truncate/multi_truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/multi_truncate_writer_scheduler.h"
#include "indexlib/index/normal/inverted_index/truncate/simple_truncate_writer_scheduler.h"
#include "indexlib/index/normal/inverted_index/truncate/trunc_work_item.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiTruncateIndexWriter);

MultiTruncateIndexWriter::MultiTruncateIndexWriter() 
{
    uint32_t threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    char *threadCountEnv = getenv("TRUNCATE_THREAD_COUNT");
    if (threadCountEnv)
    {
        autil::StringUtil::fromString(threadCountEnv, threadCount);
    }
    SetTruncateThreadCount(threadCount);
}

MultiTruncateIndexWriter::~MultiTruncateIndexWriter() 
{
    mScheduler.reset();
    mIndexWriters.clear();
}

void MultiTruncateIndexWriter::Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    for (size_t i = 0; i < mIndexWriters.size(); i++)
    {
        mIndexWriters[i]->Init(outputSegmentMergeInfos);
    }
}

void MultiTruncateIndexWriter::AddPosting(dictkey_t dictKey, 
        const PostingIteratorPtr& postingIt, df_t docFreq)
{
    assert(postingIt);
    TruncateTriggerInfo info(dictKey, docFreq);
    size_t indexWriterCount = mIndexWriters.size();
    
    for (size_t i = 0; i < indexWriterCount; i++)
    {
        if (mIndexWriters[i]->NeedTruncate(info))
        {
            PostingIteratorPtr it(postingIt->Clone());
            TruncWorkItem *workItem = new TruncWorkItem(dictKey, it, mIndexWriters[i]);
            mScheduler->PushWorkItem(workItem);
        }
    }
    mScheduler->WaitFinished();
}

bool MultiTruncateIndexWriter::NeedTruncate(const TruncateTriggerInfo &info) const
{
    mEstimatePostingCount = 0;
    bool ret = false;
    for (size_t i = 0; i < mIndexWriters.size(); i++)
    {
        if (mIndexWriters[i]->NeedTruncate(info))
        {
            ++mEstimatePostingCount;
            ret = true;
        }
    }
    return ret;
}

void MultiTruncateIndexWriter::AddIndexWriter(
        const TruncateIndexWriterPtr& indexWriter)
{
    mIndexWriters.push_back(indexWriter);
}

void MultiTruncateIndexWriter::EndPosting()
{
    for (size_t i = 0; i < mIndexWriters.size(); i++)
    {
        mIndexWriters[i]->EndPosting();
    }
    mScheduler->WaitFinished();
    mIndexWriters.clear();
    IE_LOG(DEBUG, "End Posting");    
}

size_t MultiTruncateIndexWriter::GetInternalWriterCount() const
{
    return mIndexWriters.size();
}

void MultiTruncateIndexWriter::SetTruncateThreadCount(uint32_t threadCount)
{
    if (threadCount == 0 || threadCount > 512)
    {
        IE_LOG(WARN, "invalid truncate thread count [%u], use default [%u]",
               threadCount, DEFAULT_TRUNC_THREAD_COUNT);
        threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    }
    
    IE_LOG(INFO, "merge truncate index use %d thread", threadCount);
    if (threadCount > 1)
    {
        IE_LOG(INFO, "init multi thread truncate scheduler!");
        mScheduler.reset(new MultiTruncateWriterScheduler(threadCount));
    }
    else
    {
        IE_LOG(INFO, "init simple truncate scheduler!");
        mScheduler.reset(new SimpleTruncateWriterScheduler());
    }
}

TruncateWriterSchedulerPtr MultiTruncateIndexWriter::getTruncateWriterScheduler() const
{
    return mScheduler;
}

int64_t MultiTruncateIndexWriter::EstimateMemoryUse(
        int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const
{
    vector<int64_t> memUseVec(mIndexWriters.size(), 0);
    for (size_t i = 0; i < mIndexWriters.size(); i++)
    {
        memUseVec[i] = mIndexWriters[i]->EstimateMemoryUse(
                maxPostingLen, totalDocCount, outputSegmentCount);
    }
    sort(memUseVec.begin(), memUseVec.end());
    reverse(memUseVec.begin(), memUseVec.end());
    uint32_t threadCount = 1;
    MultiTruncateWriterScheduler *multiScheduler = 
        dynamic_cast<MultiTruncateWriterScheduler *>(mScheduler.get());
    if (multiScheduler)
    {
        threadCount = multiScheduler->GetThreadCount();
    }
    
    int64_t memUse = 0;
    for (uint32_t i = 0; i < memUseVec.size() && i < threadCount; i++)
    {
        memUse += memUseVec[i];
    }
    return memUse;
}

IE_NAMESPACE_END(index);

