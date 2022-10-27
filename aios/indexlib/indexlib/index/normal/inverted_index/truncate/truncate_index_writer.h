#ifndef __INDEXLIB_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_TRUNCATE_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

IE_NAMESPACE_BEGIN(index);

class TruncateIndexWriter
{
public:
    TruncateIndexWriter() 
        : mEstimatePostingCount(0) 
    {
    }
    virtual ~TruncateIndexWriter() {}

public:
    virtual void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) = 0;
    virtual bool NeedTruncate(const TruncateTriggerInfo &info) const = 0;
    virtual void AddPosting(dictkey_t dictKey, 
                            const PostingIteratorPtr& postingIt,
                            df_t docFreq = -1) = 0;
    virtual void EndPosting() = 0;
    virtual int32_t GetEstimatePostingCount()
    {
        return mEstimatePostingCount;
    };
    virtual void SetIOConfig(const config::MergeIOConfig &ioConfig) {}
    virtual int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
            size_t outputSegmentCount) const = 0;

protected:
    mutable int32_t mEstimatePostingCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_INDEX_WRITER_H
