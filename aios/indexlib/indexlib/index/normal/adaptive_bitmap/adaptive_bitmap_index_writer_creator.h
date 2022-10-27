#ifndef __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H
#define __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger_creator.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/config/merge_io_config.h"

DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapIndexWriter);
DECLARE_REFERENCE_CLASS(index, MultiAdaptiveBitmapIndexWriter);


IE_NAMESPACE_BEGIN(index);

class AdaptiveBitmapIndexWriterCreator
{
public:
    AdaptiveBitmapIndexWriterCreator(
            const storage::ArchiveFolderPtr& adaptiveDictFolder, 
            const ReclaimMapPtr &reclaimMap,
            const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos);
    virtual ~AdaptiveBitmapIndexWriterCreator();

public:
    MultiAdaptiveBitmapIndexWriterPtr Create(
            const config::IndexConfigPtr& indexConf,
            const config::MergeIOConfig& ioConfig);

private:
    // virtual for test
    virtual AdaptiveBitmapTriggerPtr CreateAdaptiveBitmapTrigger(
            const config::IndexConfigPtr& indexConf);

private:
    storage::ArchiveFolderPtr mAdaptiveDictFolder;
    AdaptiveBitmapTriggerCreator mTriggerCreator;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapIndexWriterCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_CREATOR_H
