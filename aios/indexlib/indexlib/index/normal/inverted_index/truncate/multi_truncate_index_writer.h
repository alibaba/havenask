#ifndef __INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H

#include <tr1/memory>
#include <autil/OutputOrderedThreadPool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_writer_scheduler.h"

IE_NAMESPACE_BEGIN(index);

class MultiTruncateIndexWriter : public TruncateIndexWriter
{
public:
    typedef std::vector<TruncateIndexWriterPtr> IndexWriterList;
public:
    MultiTruncateIndexWriter();
    ~MultiTruncateIndexWriter();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) override;
    bool NeedTruncate(const TruncateTriggerInfo &info) const override;
    void AddPosting(dictkey_t dictKey, const PostingIteratorPtr& postingIt, 
                    df_t docFreq = -1) override;
    void EndPosting() override;
    void SetTruncateThreadCount(uint32_t threadCount);
    size_t GetInternalWriterCount() const; 
    void AddIndexWriter(const TruncateIndexWriterPtr& indexWriter);

    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                              size_t outputSegmentCount) const override;

public:
    // for test
    TruncateWriterSchedulerPtr getTruncateWriterScheduler() const;

private:
    void CreateTruncateWriteScheduler(bool syncTrunc);

private:
    IndexWriterList mIndexWriters;
    TruncateWriterSchedulerPtr mScheduler;
    static const uint32_t DEFAULT_TRUNC_THREAD_COUNT = 2;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiTruncateIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H
