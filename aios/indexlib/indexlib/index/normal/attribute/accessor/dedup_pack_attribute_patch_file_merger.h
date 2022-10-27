#ifndef __INDEXLIB_DEDUP_PACK_ATTRIBUTE_PATCH_FILE_MERGER_H
#define __INDEXLIB_DEDUP_PACK_ATTRIBUTE_PATCH_FILE_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(index);

class DedupPackAttributePatchFileMerger
{
public:
    DedupPackAttributePatchFileMerger(
        const config::PackAttributeConfigPtr& packAttrConfig)
        : mPackAttrConfig(packAttrConfig)
    {}
    
    ~DedupPackAttributePatchFileMerger() {}
    
public:
    void Merge(const index_base::PartitionDataPtr& partitionData,
               const index_base::SegmentMergeInfos& segMergeInfos,
               const file_system::DirectoryPtr& targetAttrDir);

    uint32_t EstimateMemoryUse(const index_base::PartitionDataPtr& partitionData,
                               const index_base::SegmentMergeInfos& segMergeInfos);
    
private:
    config::PackAttributeConfigPtr mPackAttrConfig;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DedupPackAttributePatchFileMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEDUP_PACK_ATTRIBUTE_PATCH_FILE_MERGER_H
