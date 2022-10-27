#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, RealtimeSegmentDirectory);

RealtimeSegmentDirectory::RealtimeSegmentDirectory(bool enableRecover)
    : mEnableRecover(enableRecover)
{
}

RealtimeSegmentDirectory::~RealtimeSegmentDirectory() 
{
}

void RealtimeSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    if (mEnableRecover)
    {
        RemoveVersionFiles(directory);
        mVersion = INVALID_VERSION;
        mVersion = OfflineRecoverStrategy::Recover(mVersion, directory);
        mVersion.SetVersionId(0);
        // realtime segment no need DumpDeployIndexMeta
        mVersion.Store(directory, false);
    }
}

void RealtimeSegmentDirectory::RemoveVersionFiles(
        const file_system::DirectoryPtr& directory)
{
    fslib::FileList fileLists;
    VersionLoader::ListVersion(directory, fileLists);
    for (size_t i = 0; i < fileLists.size(); i++)
    {
        directory->RemoveFile(fileLists[i]);
    }
}

IE_NAMESPACE_END(index_base);

