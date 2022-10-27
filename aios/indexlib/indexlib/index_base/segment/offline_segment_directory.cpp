#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/offline_recover_strategy.h"

using namespace std;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, OfflineSegmentDirectory);

OfflineSegmentDirectory::OfflineSegmentDirectory(
    bool enableRecover,
    RecoverStrategyType recoverType)
    : mEnableRecover(enableRecover)
    , mRecoverType(recoverType)
{
}

OfflineSegmentDirectory::OfflineSegmentDirectory(const OfflineSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEnableRecover(other.mEnableRecover)
    , mRecoverType(other.mRecoverType)
{
}

OfflineSegmentDirectory::~OfflineSegmentDirectory() 
{
}

void OfflineSegmentDirectory::DoInit(const file_system::DirectoryPtr& directory)
{
    if (!mIsSubSegDir && mEnableRecover)
    {
        mVersion = OfflineRecoverStrategy::Recover(mVersion, directory, mRecoverType);
    }
}

IE_NAMESPACE_END(index_base);

