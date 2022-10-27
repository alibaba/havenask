#ifndef __INDEXLIB_SEGMENT_DATA_H
#define __INDEXLIB_SEGMENT_DATA_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchIndexAccessor);
DECLARE_REFERENCE_CLASS(index, OnDiskSegmentSizeCalculator);
DECLARE_REFERENCE_CLASS(index_base, SegmentFileMeta);

IE_NAMESPACE_BEGIN(index_base);

class SegmentData;
DEFINE_SHARED_PTR(SegmentData);

class SegmentData : public SegmentDataBase
{
public:
    SegmentData();
    SegmentData(const SegmentData& segData);
    virtual ~SegmentData();

public:
    void SetSegmentInfo(const SegmentInfo& segmentInfo) 
    { mSegmentInfo = segmentInfo; }
    const SegmentInfo& GetSegmentInfo() const { return mSegmentInfo; }
    void SetSegmentMetrics(const SegmentMetrics& segmentMetrics)
    {
        mSegmentMetrics = segmentMetrics;
    }
    void SetSegmentMetrics(SegmentMetrics&& segmentMetrics)
    {
        mSegmentMetrics = std::move(segmentMetrics);
    }
    const SegmentMetrics& GetSegmentMetrics() const { return mSegmentMetrics; }

    void SetPatchIndexAccessor(const PartitionPatchIndexAccessorPtr& patchAccessor)
    { mPatchAccessor = patchAccessor; }

    const PartitionPatchIndexAccessorPtr& GetPatchIndexAccessor() const
    { return mPatchAccessor; }

    file_system::DirectoryPtr GetSummaryDirectory(
            bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetIndexDirectory(const std::string& indexName,
            bool throwExceptionIfNotExist) const;

    file_system::DirectoryPtr GetSectionAttributeDirectory(const std::string& indexName,
            bool throwExceptionIfNotExist) const;

    file_system::DirectoryPtr GetAttributeDirectory(const std::string& attrName,
            bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetOperationDirectory(
            bool throwExceptionIfNotExist) const;

    void SetSubSegmentData(const SegmentDataPtr& segmentData)
    { mSubSegmentData = segmentData; }
    const SegmentDataPtr& GetSubSegmentData() const
    { return mSubSegmentData; }

    SegmentData& operator=(const SegmentData& segData);

    std::string GetShardingDirInSegment(uint32_t shardingIdx) const;
    SegmentData CreateShardingSegmentData(uint32_t shardingIdx) const;

    uint32_t GetShardingColumnId() const { return mSegmentInfo.shardingColumnId; }

    virtual bool IsBuildingSegment() const
    { return false; }

    void SetDirectory(const file_system::DirectoryPtr& directory);
    const file_system::DirectoryPtr& GetDirectory() const
    {
        return mDirectory;
    }

    void SetSegmentFileMeta(const SegmentFileMetaPtr& segmentDeployIndex)
    { mSegmentFileMeta = segmentDeployIndex; }
    
    const SegmentFileMetaPtr& GetSegmentFileMeta() const
    { return mSegmentFileMeta; }

private:
    file_system::DirectoryPtr GetIndexDirectory(
            bool throwExceptionIfNotExist) const;
    file_system::DirectoryPtr GetAttributeDirectory(
            bool throwExceptionIfNotExist) const;
protected:
    SegmentInfo mSegmentInfo;
    SegmentMetrics mSegmentMetrics;
    file_system::DirectoryPtr mDirectory;

private:
    mutable file_system::DirectoryPtr mIndexDirectory;
    mutable file_system::DirectoryPtr mAttrDirectory;
    mutable file_system::DirectoryPtr mSummaryDirectory;
    mutable file_system::DirectoryPtr mOperationDirectory;
    mutable autil::ThreadMutex mLock;
protected:
    SegmentDataPtr mSubSegmentData;
    PartitionPatchIndexAccessorPtr mPatchAccessor;
    SegmentFileMetaPtr mSegmentFileMeta;

private:
    IE_LOG_DECLARE();
};

typedef std::vector<SegmentData> SegmentDataVector;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_DATA_H
