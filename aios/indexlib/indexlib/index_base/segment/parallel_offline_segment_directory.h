#ifndef __INDEXLIB_PARALLEL_OFFLINE_SEGMENT_DIRECTORY_H
#define __INDEXLIB_PARALLEL_OFFLINE_SEGMENT_DIRECTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

IE_NAMESPACE_BEGIN(index_base);

class ParallelOfflineSegmentDirectory : public OfflineSegmentDirectory
{
public:
    ParallelOfflineSegmentDirectory(bool enableRecover = true);
    ParallelOfflineSegmentDirectory(const ParallelOfflineSegmentDirectory& other);
    ~ParallelOfflineSegmentDirectory();
public:
    segmentid_t CreateNewSegmentId() override;
    void CommitVersion() override
    { return DoCommitVersion(false); }

    ParallelOfflineSegmentDirectory* Clone() override;
private:
    void UpdateCounterMap(const util::CounterMapPtr& counterMap) const override;
protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;
    
    index_base::Version GetLatestVersion(
            const file_system::DirectoryPtr& directory,
            const index_base::Version& emptyVersion) const override;
private:
    index_base::ParallelBuildInfo mParallelBuildInfo;
    segmentid_t mStartBuildingSegId;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelOfflineSegmentDirectory);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARALLEL_OFFLINE_SEGMENT_DIRECTORY_H
