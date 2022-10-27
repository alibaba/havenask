#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, BuildingSegmentData);

BuildingSegmentData::BuildingSegmentData()
    : mIsSubSegment(false)
{
}

BuildingSegmentData::BuildingSegmentData(
        const config::BuildConfig& buildConfig)
    : mIsSubSegment(false)
    , mBuildConfig(buildConfig)
{
}

BuildingSegmentData::~BuildingSegmentData() 
{
}

BuildingSegmentData::BuildingSegmentData(const BuildingSegmentData& segData)
    : SegmentData(segData)
    , mIsSubSegment(segData.mIsSubSegment)
    , mPartitionDir(segData.mPartitionDir)
    , mBuildConfig(segData.mBuildConfig)
{
}

void BuildingSegmentData::Init(const DirectoryPtr& directory,
                               bool isSubSegment)
{
    mPartitionDir = directory;
    mIsSubSegment = isSubSegment;
    if (mSubSegmentData)
    {
        BuildingSegmentDataPtr subSegData =
            DYNAMIC_POINTER_CAST(BuildingSegmentData, mSubSegmentData);
        assert(subSegData);
        subSegData->Init(directory, true);
    }
}

void BuildingSegmentData::PrepareDirectory()
{
    if (mDirectory)
    {
        return;
    }
    
    string segDirName = GetSegmentDirName();
    assert(!segDirName.empty());
    DirectoryPtr segDirectory;
    try
    {
        segDirectory = mPartitionDir->MakeInMemDirectory(segDirName);
    }
    catch (const misc::ExceptionBase& e)
    {
        IE_LOG(WARN, "MakeInMemDirectory for [%s] fail : reason [%s]", segDirName.c_str(), e.what());
        segDirectory = mPartitionDir->GetDirectory(segDirName, true);
    }
    if (!mIsSubSegment)
    {
        SetDirectory(segDirectory);
        SwitchToPackDirectory();
        return;
    }
    
    assert(mIsSubSegment);
    if (!segDirectory->IsExist(SUB_SEGMENT_DIR_NAME))
    {
        segDirectory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    }
    SetDirectory(segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, true));
    SwitchToPackDirectory();
}

BuildingSegmentData BuildingSegmentData::CreateShardingSegmentData(uint32_t idx)
{
    if (mIsSubSegment)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "sub segment not support sharding column!");
    }
    
    if (!mDirectory)
    {
        PrepareDirectory();
    }
    assert(mDirectory);
    string dirName = SegmentData::GetShardingDirInSegment(idx);
    if (!mDirectory->IsExist(dirName))
    {
        //TODO: test create in mem directory in offline
        mDirectory->MakeInMemDirectory(dirName);
    }

    DirectoryPtr directory = mDirectory->GetDirectory(dirName, true);
    BuildingSegmentData segData(*this);
    segData.SetDirectory(directory);
    return segData;
}

void BuildingSegmentData::SwitchToPackDirectory()
{
    mUnpackDirectory = GetDirectory();
    if (!mBuildConfig.enablePackageFile || !mUnpackDirectory)
    {
        return;
    }
    PackDirectoryPtr packDirectory =
        mUnpackDirectory->CreatePackDirectory(PACKAGE_FILE_PREFIX);
    SetDirectory(packDirectory);
}

IE_NAMESPACE_END(index_base);

