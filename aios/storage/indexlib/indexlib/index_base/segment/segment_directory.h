/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H
#define __INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/CounterMap.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, LifecycleConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchIndexAccessor);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentTemperatureMeta);
DECLARE_REFERENCE_CLASS(config, BuildConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

namespace indexlibv2 { namespace framework {
class SegmentStatistics;
}} // namespace indexlibv2::framework
namespace indexlib { namespace index_base {

class LifecycleStrategy;
typedef std::vector<SegmentData> SegmentDataVector;
class SegmentDirectory;
DEFINE_SHARED_PTR(SegmentDirectory);

class LifecycleStrategy;

class SegmentDirectory
{
public:
    class SegmentDataFinder
    {
    public:
        SegmentDataFinder(const SegmentDataVector& segmentDatas) : mSegDataVec(segmentDatas), mCursor(0) {}

        bool Find(segmentid_t segId, SegmentData& segData);

    private:
        const SegmentDataVector& mSegDataVec;
        size_t mCursor;
    };

public:
    SegmentDirectory(std::shared_ptr<file_system::LifecycleConfig> lifecycleConfig = nullptr);
    SegmentDirectory(const SegmentDirectory& other);
    virtual ~SegmentDirectory();

public:
    void Init(const file_system::DirectoryPtr& directory,
              index_base::Version version = index_base::Version(INVALID_VERSION), bool hasSub = false);

    void Reopen(const index_base::Version& version);

    const index_base::Version& GetVersion() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mVersion;
    }
    const index_base::Version& GetOnDiskVersion() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mOnDiskVersion;
    }
    const indexlibv2::framework::VersionDeployDescriptionPtr& GetVersionDeployDescription() const
    {
        return mOnDiskVersionDpDesc;
    }
    void SetVersionDeployDescription(const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc)
    {
        mOnDiskVersionDpDesc = versionDpDesc;
    }

    const file_system::DirectoryPtr& GetRootDirectory() const { return mRootDirectory; }

    virtual SegmentDirectory* Clone();
    virtual file_system::DirectoryPtr GetSegmentParentDirectory(segmentid_t segId) const;
    virtual file_system::DirectoryPtr GetSegmentFsDirectory(segmentid_t segId) const;

    virtual segmentid_t FormatSegmentId(segmentid_t segId) const { return segId; }
    virtual bool IsMatchSegmentId(segmentid_t segId) const { return true; }

    virtual void IncLastSegmentId();

    virtual void UpdateSchemaVersionId(schemavid_t id);
    virtual void SetOngoingModifyOperations(const std::vector<schema_opid_t>& opIds);

    virtual void AddSegment(segmentid_t segId, timestamp_t ts);

    virtual void AddSegment(segmentid_t segId, timestamp_t ts,
                            const std::shared_ptr<indexlibv2::framework::SegmentStatistics>& segmentStats);

    virtual void AddSegmentTemperatureMeta(const SegmentTemperatureMeta& temperatureMeta);
    virtual void RemoveSegments(const std::vector<segmentid_t>& segIds);
    virtual bool IsVersionChanged() const;
    virtual void CommitVersion(const config::IndexPartitionOptions& options);
    virtual segmentid_t CreateNewSegmentId();
    virtual segmentid_t GetNextSegmentId(segmentid_t baseSegmentId) const;
    virtual BuildingSegmentData CreateNewSegmentData(const config::BuildConfig& buildConfig);
    virtual void SetSubSegmentDir();

    virtual const index_base::IndexFormatVersion& GetIndexFormatVersion() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mIndexFormatVersion;
    }

    virtual segmentid_t GetLastValidRtSegmentInLinkDirectory() const { return INVALID_SEGMENTID; }

    virtual bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) { return false; }

    virtual void UpdatePatchAccessor(const index_base::Version& version);
    virtual void UpdateCounterMap(const util::CounterMapPtr& counterMap) const;
    virtual std::string GetLastLocator() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mVersion.GetLocator().ToString();
    }

    std::string GetSegmentDirName(segmentid_t segId) const;

    SegmentDataVector& GetSegmentDatas()
    {
        autil::ScopedLock scopedLock(mLock);
        return mSegmentDatas;
    }
    const SegmentDataVector& GetSegmentDatas() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mSegmentDatas;
    }
    SegmentData GetSegmentData(segmentid_t segId) const;
    std::pair<bool, std::string> GetSegmentLifecycle(segmentid_t segId) const;

    bool IsSubSegmentDirectory() const { return mIsSubSegDir; }

    SegmentDirectoryPtr GetSubSegmentDirectory();
    void RollbackToCurrentVersion(bool needRemoveSegment = true);

protected:
    virtual void DoInit(const file_system::DirectoryPtr& directory) {}
    virtual void DoReopen() { assert(false); }
    virtual std::shared_ptr<index_base::LifecycleStrategy> InitLifecycleStrategy();
    virtual void UpdateLifecycleStrategy();
    virtual void RefreshSegmentLifecycle(bool isNewSegment, SegmentData* segmentData);

    void InitSegmentDatas(index_base::Version& version);
    void InitIndexFormatVersion(const file_system::DirectoryPtr& directory);
    virtual void DoCommitVersion(const config::IndexPartitionOptions& options, bool dumpDeployIndexMeta);

    virtual index_base::Version GetLatestVersion(const file_system::DirectoryPtr& directory,
                                                 const index_base::Version& emptyVersion) const;

protected:
    std::shared_ptr<file_system::LifecycleConfig> mLifecycleConfig;
    std::shared_ptr<LifecycleStrategy> mLifecycleStrategy;

    index_base::Version mVersion;
    index_base::Version mOnDiskVersion;

    file_system::DirectoryPtr mRootDirectory;
    SegmentDataVector mSegmentDatas;
    index_base::IndexFormatVersion mIndexFormatVersion;

    bool mIsSubSegDir;
    SegmentDirectoryPtr mSubSegmentDirectory;
    PartitionPatchIndexAccessorPtr mPatchAccessor;
    mutable autil::RecursiveThreadMutex mLock;
    indexlibv2::framework::VersionDeployDescriptionPtr mOnDiskVersionDpDesc;

private:
    friend class SegmentDirectoryTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H
