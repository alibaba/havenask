#ifndef __INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H
#define __INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/index_format_version.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchIndexAccessor);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(config, BuildConfig);

IE_NAMESPACE_BEGIN(index_base);

typedef std::vector<SegmentData> SegmentDataVector;

class SegmentDirectory;
DEFINE_SHARED_PTR(SegmentDirectory);

class SegmentFileMeta;
DEFINE_SHARED_PTR(SegmentFileMeta);

class SegmentDirectory
{
public:
    class SegmentDataFinder
    {
    public:
        SegmentDataFinder(const SegmentDataVector& segmentDatas)
            : mSegDataVec(segmentDatas)
            , mCursor(0)
        {}

        bool Find(segmentid_t segId, SegmentData& segData);
        
    private:
        const SegmentDataVector& mSegDataVec;
        size_t mCursor;
    };
    
public:
    SegmentDirectory();
    SegmentDirectory(const SegmentDirectory& other);
    virtual ~SegmentDirectory();

public:
    void Init(const file_system::DirectoryPtr& directory,
              index_base::Version version = INVALID_VERSION,
              bool hasSub = false);

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
    const file_system::DirectoryPtr& GetRootDirectory() const 
    {
        autil::ScopedLock scopedLock(mLock);
        return mRootDirectory;
    }

    virtual SegmentDirectory* Clone();
    virtual file_system::DirectoryPtr GetSegmentParentDirectory(
            segmentid_t segId) const;
    virtual file_system::DirectoryPtr GetSegmentFsDirectory(
        segmentid_t segId, SegmentFileMetaPtr& segmentFileMeta) const;

    virtual segmentid_t FormatSegmentId(segmentid_t segId) const
    { return segId; }
    virtual bool IsMatchSegmentId(segmentid_t segId) const
    { return true; }

    virtual void IncLastSegmentId();

    virtual void UpdateSchemaVersionId(schemavid_t id);
    virtual void SetOngoingModifyOperations(const std::vector<schema_opid_t>& opIds);
    
    virtual void AddSegment(segmentid_t segId, timestamp_t ts);
    virtual void RemoveSegments(const std::vector<segmentid_t>& segIds);
    virtual bool IsVersionChanged() const;
    virtual void CommitVersion();
    virtual segmentid_t CreateNewSegmentId();
    virtual BuildingSegmentData CreateNewSegmentData(
            const config::BuildConfig& buildConfig);
    virtual void SetSubSegmentDir();

    virtual const index_base::IndexFormatVersion& GetIndexFormatVersion() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mIndexFormatVersion;
    }

    virtual segmentid_t GetLastValidRtSegmentInLinkDirectory() const
    { return INVALID_SEGMENTID; }
    
    virtual bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
    { return false; }

    virtual void UpdatePatchAccessor(const index_base::Version& version);
    virtual void UpdateCounterMap(const util::CounterMapPtr& counterMap) const;
    virtual std::string GetLastLocator() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mVersion.GetLocator().ToString();
    }
    
    segmentid_t ExtractSegmentId(const std::string& path);
    std::string GetSegmentDirName(segmentid_t segId) const;

    const SegmentDataVector& GetSegmentDatas() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mSegmentDatas;
    }
    SegmentData GetSegmentData(segmentid_t segId) const;

    bool IsSubSegmentDirectory() const { return mIsSubSegDir; }

    SegmentDirectoryPtr GetSubSegmentDirectory();
    void RollbackToCurrentVersion(bool needRemoveSegment = true);

protected:
    virtual void DoInit(const file_system::DirectoryPtr& directory) {}
    void InitSegmentDatas(index_base::Version& version);
    void InitIndexFormatVersion(const file_system::DirectoryPtr& directory);
    void DoCommitVersion(bool dumpDeployIndexMeta);

    virtual index_base::Version GetLatestVersion(
            const file_system::DirectoryPtr& directory,
            const index_base::Version& emptyVersion) const;
    
protected:
    index_base::Version mVersion;
    index_base::Version mOnDiskVersion;

    std::string mRootDir;
    file_system::DirectoryPtr mRootDirectory;
    SegmentDataVector mSegmentDatas;
    index_base::IndexFormatVersion mIndexFormatVersion;

    bool mIsSubSegDir;
    SegmentDirectoryPtr mSubSegmentDirectory;
    PartitionPatchIndexAccessorPtr mPatchAccessor;
    mutable autil::RecursiveThreadMutex mLock;

private:
    friend class SegmentDirectoryTest;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITION_SEGMENT_DIRECTORY_H
