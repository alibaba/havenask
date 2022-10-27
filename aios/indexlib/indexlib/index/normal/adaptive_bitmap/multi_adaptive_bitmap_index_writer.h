#ifndef __INDEXLIB_MULTI_ADAPTIVE_BITMAP_INDEX_WRITER_H
#define __INDEXLIB_MULTI_ADAPTIVE_BITMAP_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_data_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_iterator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"

IE_NAMESPACE_BEGIN(index);

class MultiAdaptiveBitmapIndexWriter
{
public:
    MultiAdaptiveBitmapIndexWriter(const AdaptiveBitmapTriggerPtr& trigger,
                                   const config::IndexConfigPtr& indexConf,
                                   const storage::ArchiveFolderPtr& adaptiveDictFolder, 
                                   const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
        : mAdaptiveDictFolder(adaptiveDictFolder)
        , mIndexConfig(indexConf)
    {
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++)
        {
            AdaptiveBitmapIndexWriterPtr adaptiveBitmapIndexWriter(
                new AdaptiveBitmapIndexWriter(trigger, indexConf, adaptiveDictFolder));

            mAdaptiveBitmapIndexWriters.push_back(adaptiveBitmapIndexWriter);
        }
    }
    ~MultiAdaptiveBitmapIndexWriter();

public:
    void Init(IndexOutputSegmentResources& indexOutputSegmentResources)
    {
        assert(indexOutputSegmentResources.size() == mAdaptiveBitmapIndexWriters.size());
        for (size_t i = 0; i < indexOutputSegmentResources.size(); i++)
        {
            IndexDataWriterPtr bitmapIndexDataWriter
                = indexOutputSegmentResources[i]->GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);

            mAdaptiveBitmapIndexWriters[i]->Init(
                bitmapIndexDataWriter->dictWriter, bitmapIndexDataWriter->postingWriter);
        }
    }

    void EndPosting();
    bool NeedAdaptiveBitmap(dictkey_t dictKey, const PostingMergerPtr& postingMerger)
    {
        return mAdaptiveBitmapIndexWriters[0]->NeedAdaptiveBitmap(dictKey, postingMerger);
    }

    virtual void AddPosting(
        dictkey_t dictKey, termpayload_t termPayload, const PostingIteratorPtr& postingIt)
    {
        MultiSegmentPostingIteratorPtr multiIter = DYNAMIC_POINTER_CAST(MultiSegmentPostingIterator, postingIt);
        for (size_t i = 0; i < multiIter->GetSegmentPostingCount(); i++)
        {
            segmentid_t segmentIdx;
            auto postingIter = multiIter->GetSegmentPostingIter(i, segmentIdx);
            mAdaptiveBitmapIndexWriters[segmentIdx]->AddPosting(dictKey, termPayload, postingIter);
        }
    }

    int64_t EstimateMemoryUse(uint32_t docCount) const
    {
        int64_t size = sizeof(AdaptiveBitmapIndexWriter) * mAdaptiveBitmapIndexWriters.size();
        size += docCount * sizeof(docid_t) * 2;
        size += file_system::FSWriterParam::DEFAULT_BUFFER_SIZE * 2 * mAdaptiveBitmapIndexWriters.size(); // data and dict
        return size;
    }

public:
    AdaptiveBitmapIndexWriterPtr GetSingleAdaptiveBitmapWriter(uint32_t idx)
    {
        return mAdaptiveBitmapIndexWriters[idx];
    }
private : 
    std::vector<AdaptiveBitmapIndexWriterPtr> mAdaptiveBitmapIndexWriters;
    storage::ArchiveFolderPtr mAdaptiveDictFolder;
    config::IndexConfigPtr mIndexConfig;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiAdaptiveBitmapIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_ADAPTIVE_BITMAP_INDEX_WRITER_H
