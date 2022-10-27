#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_config.h"
#include "indexlib/util/path_util.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AdaptiveBitmapIndexWriterCreator);

AdaptiveBitmapIndexWriterCreator::AdaptiveBitmapIndexWriterCreator(
        const storage::ArchiveFolderPtr& adaptiveDictFolder, 
        const ReclaimMapPtr& reclaimMap,
        const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
    : mTriggerCreator(reclaimMap)
{
    mAdaptiveDictFolder = adaptiveDictFolder;
    mOutputSegmentMergeInfos = outputSegmentMergeInfos;
}

AdaptiveBitmapIndexWriterCreator::~AdaptiveBitmapIndexWriterCreator() 
{
}

AdaptiveBitmapTriggerPtr 
AdaptiveBitmapIndexWriterCreator::CreateAdaptiveBitmapTrigger(
        const IndexConfigPtr& indexConf)
{
    return mTriggerCreator.Create(indexConf);
}

MultiAdaptiveBitmapIndexWriterPtr AdaptiveBitmapIndexWriterCreator::Create(
        const config::IndexConfigPtr& indexConf,
        const config::MergeIOConfig& ioConfig)
{
    if (indexConf->GetShardingType() == IndexConfig::IST_NEED_SHARDING)
    {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }
 
    AdaptiveBitmapTriggerPtr trigger = 
        CreateAdaptiveBitmapTrigger(indexConf);
    if (!trigger)
    {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }
        
    MultiAdaptiveBitmapIndexWriterPtr writer;
    if (it_unknown == indexConf->GetIndexType())
    {
        return MultiAdaptiveBitmapIndexWriterPtr();
    }

    // if (!FileSystemWrapper::IsExist(mAdaptiveDictDir))
    // {
    //     PrepareAdaptiveDictDir(); 
    // }
    writer.reset(new MultiAdaptiveBitmapIndexWriter(
                    trigger, indexConf, mAdaptiveDictFolder, mOutputSegmentMergeInfos));
    return writer;
}


IE_NAMESPACE_END(index);

