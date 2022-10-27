#ifndef __INDEXLIB_BUILDING_SEGMENT_DATA_H
#define __INDEXLIB_BUILDING_SEGMENT_DATA_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/config/build_config.h"

IE_NAMESPACE_BEGIN(index_base);

class BuildingSegmentData : public SegmentData
{
public:
    BuildingSegmentData();
    BuildingSegmentData(const config::BuildConfig& buildConfig);
    BuildingSegmentData(const BuildingSegmentData& segData);
    ~BuildingSegmentData();
    
public:
    void Init(const file_system::DirectoryPtr& directory, bool isSubSegment = false);

    void PrepareDirectory();

    BuildingSegmentData CreateShardingSegmentData(uint32_t idx);

    file_system::DirectoryPtr GetUnpackDirectory() const
    { return mUnpackDirectory; }

    bool IsBuildingSegment() const override { return true; }

private:
    void SwitchToPackDirectory();
    
private:
    bool mIsSubSegment;
    file_system::DirectoryPtr mPartitionDir;
    file_system::DirectoryPtr mUnpackDirectory;
    config::BuildConfig mBuildConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentData);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_BUILDING_SEGMENT_DATA_H
