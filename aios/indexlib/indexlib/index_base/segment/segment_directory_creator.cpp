#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/index_base/segment/parallel_offline_segment_directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentDirectoryCreator);

SegmentDirectoryCreator::SegmentDirectoryCreator() 
{
}

SegmentDirectoryCreator::~SegmentDirectoryCreator() 
{
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(
        const config::IndexPartitionOptions& options,
        const file_system::DirectoryPtr& directory,
        const index_base::Version& version,
        bool hasSub)
{
    SegmentDirectoryPtr segDir = Create(&options);
    segDir->Init(directory, version, hasSub);
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(
        const file_system::IndexlibFileSystemPtr& fileSystem,
        index_base::Version version,
        const std::string& dir, 
        const config::IndexPartitionOptions* options,
        bool hasSub)
{
    string partitionRootDir = dir;
    if (dir.empty())
    {
        partitionRootDir = fileSystem->GetRootPath();
    }
    bool isParallelBuild = false;
    if (ParallelBuildInfo::IsExist(partitionRootDir))
    {
        isParallelBuild = true;
    }
    DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem,
            partitionRootDir, true);
    SegmentDirectoryPtr segDir = Create(options, isParallelBuild);
    segDir->Init(rootDirectory, version, hasSub);
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(
        const config::IndexPartitionOptions* options, bool isParallelBuild)
{
    SegmentDirectoryPtr segDir;
    if (!options)
    {
        segDir.reset(new SegmentDirectory);
    }
    else if (options->IsOnline())
    {
        segDir.reset(new OnlineSegmentDirectory(
                        options->GetOnlineConfig().enableRecoverIndex));
    }
    else if (isParallelBuild)
    {
        segDir.reset(new ParallelOfflineSegmentDirectory(
                        options->GetOfflineConfig().enableRecoverIndex));
    }
    else
    {
        segDir.reset(new OfflineSegmentDirectory(
                         options->GetOfflineConfig().enableRecoverIndex,
                         options->GetOfflineConfig().recoverType));
    }
    return segDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(
        const file_system::DirectoryVector& directoryVec,
        bool hasSub)
{
    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory);
    multiSegDir->Init(directoryVec, hasSub);
    return multiSegDir;
}

SegmentDirectoryPtr SegmentDirectoryCreator::Create(
        const file_system::DirectoryVector& directoryVec,
        const std::vector<index_base::Version>& versions,
        bool hasSub)
{
    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory);
    multiSegDir->Init(directoryVec, versions, hasSub);
    return multiSegDir;
}

IE_NAMESPACE_END(index_base);

