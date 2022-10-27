#ifndef __INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H
#define __INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_directory.h"

IE_NAMESPACE_BEGIN(index_base);

class OnlineSegmentDirectory : public SegmentDirectory
{
public:
    OnlineSegmentDirectory(bool enableRecover = false);
    OnlineSegmentDirectory(const OnlineSegmentDirectory& other);
    ~OnlineSegmentDirectory();

public:
    SegmentDirectory* Clone() override;

    void AddSegment(segmentid_t segId, timestamp_t ts) override;
    void RemoveSegments(const std::vector<segmentid_t>& segIds) override;

    void IncLastSegmentId() override {
        autil::ScopedLock scopedLock(mLock);
        mRtSegDir->IncLastSegmentId();
    };

    bool IsVersionChanged() const override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->IsVersionChanged() 
            || mJoinSegDir->IsVersionChanged(); 
    }

    void CommitVersion() override;

    file_system::DirectoryPtr GetSegmentParentDirectory(
            segmentid_t segId) const override;

    void SetSubSegmentDir() override;

    BuildingSegmentData CreateNewSegmentData(
            const config::BuildConfig& buildConfig) override;

    segmentid_t CreateNewSegmentId() override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->CreateNewSegmentId();
    }

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) override;

    std::string GetLastLocator() const override
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir->GetLastLocator();
    }
    
    virtual segmentid_t CreateJoinSegmentId()
    {
        autil::ScopedLock scopedLock(mLock);
        return mJoinSegDir->CreateNewSegmentId();
    }

    const SegmentDirectoryPtr& GetRtSegmentDirectory() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mRtSegDir;
    }

    const SegmentDirectoryPtr& GetJoinSegmentDirectory() const
    {
        autil::ScopedLock scopedLock(mLock);
        return mJoinSegDir;
    }

    static bool IsIncSegmentId(segmentid_t segId);
    
protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

    SegmentDirectoryPtr GetSegmentDirectory(segmentid_t segId) const;
    void MergeVersion(const index_base::Version& version);
    virtual void RefreshVersion();
    
private:
    SegmentDirectoryPtr mRtSegDir;
    SegmentDirectoryPtr mJoinSegDir;
    bool mEnableRecover;
    
private:
    friend class OnlineSegmentDirectoryTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlineSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_ONLINE_SEGMENT_DIRECTORY_H
