#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentData);

SegmentData::SegmentData()
    : SegmentDataBase()
{
}

SegmentData::SegmentData(const SegmentData& segData)
{
    *this = segData;
}

SegmentData::~SegmentData() 
{
}

DirectoryPtr SegmentData::GetIndexDirectory(const string& indexName,
        bool throwExceptionIfNotExist) const
{
    DirectoryPtr indexDirectory;
    if (mPatchAccessor)
    {
        indexDirectory = mPatchAccessor->GetIndexDirectory(
                mSegmentId, indexName, throwExceptionIfNotExist);
        if (indexDirectory)
        {
            return indexDirectory;
        }
    }
    
    indexDirectory = GetIndexDirectory(throwExceptionIfNotExist);
    if (!indexDirectory)
    {
        return DirectoryPtr();
    }
    return indexDirectory->GetDirectory(indexName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetSectionAttributeDirectory(const string& indexName,
        bool throwExceptionIfNotExist) const
{
    DirectoryPtr sectionAttrDir;
    if (mPatchAccessor)
    {
        sectionAttrDir = mPatchAccessor->GetSectionAttributeDirectory(
                mSegmentId, indexName, throwExceptionIfNotExist);
        if (sectionAttrDir)
        {
            return sectionAttrDir;
        }
    }
    
    DirectoryPtr indexDirectory = GetIndexDirectory(throwExceptionIfNotExist);
    if (!indexDirectory)
    {
        return DirectoryPtr();
    }
    string sectionAttrName =
        SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    return indexDirectory->GetDirectory(sectionAttrName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetAttributeDirectory(const std::string& attrName,
        bool throwExceptionIfNotExist) const
{
    DirectoryPtr attrDirectory;
    if (mPatchAccessor)
    {
        attrDirectory = mPatchAccessor->GetAttributeDirectory(
                mSegmentId, attrName, throwExceptionIfNotExist);
        if (attrDirectory)
        {
            return attrDirectory;
        }
    }
    attrDirectory = GetAttributeDirectory(throwExceptionIfNotExist);
    if (!attrDirectory)
    {
        return DirectoryPtr();
    }
    return attrDirectory->GetDirectory(attrName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetOperationDirectory(
        bool throwExceptionIfNotExist) const
{
    ScopedLock lock(mLock);
    
    if (unlikely(!mOperationDirectory))
    {
        assert(mDirectory);
        mOperationDirectory = mDirectory->GetDirectory(
                OPERATION_DIR_NAME, throwExceptionIfNotExist);
    }
    return mOperationDirectory;
}


void SegmentData::SetDirectory(const DirectoryPtr& directory)
{
    mDirectory = directory;
    // if (mDirectory)
    // {
    //     mIndexDirectory = mDirectory->GetDirectory(INDEX_DIR_NAME, false);
    //     mAttrDirectory = mDirectory->GetDirectory(ATTRIBUTE_DIR_NAME, false);
    //     mSummaryDirectory = mDirectory->GetDirectory(SUMMARY_DIR_NAME, false);
    // }
}

DirectoryPtr SegmentData::GetIndexDirectory(bool throwExceptionIfNotExist) const
{ 
    ScopedLock lock(mLock);
    if (unlikely(!mIndexDirectory))
    {
        assert(mDirectory);
        mIndexDirectory = mDirectory->GetDirectory(
                INDEX_DIR_NAME, throwExceptionIfNotExist);
    }
    return mIndexDirectory;
}

DirectoryPtr SegmentData::GetAttributeDirectory(
        bool throwExceptionIfNotExist) const
{ 
    ScopedLock lock(mLock);
    if (unlikely(!mAttrDirectory))
    {
        assert(mDirectory);
        mAttrDirectory = mDirectory->GetDirectory(
                ATTRIBUTE_DIR_NAME, throwExceptionIfNotExist);
    }
    return mAttrDirectory;
}

DirectoryPtr SegmentData::GetSummaryDirectory(bool throwExceptionIfNotExist) const
{ 
    ScopedLock lock(mLock);
    if (unlikely(!mSummaryDirectory))
    {
        assert(mDirectory);
        mSummaryDirectory = mDirectory->GetDirectory(
                SUMMARY_DIR_NAME, throwExceptionIfNotExist);
    }
    return mSummaryDirectory; 
}

SegmentData& SegmentData::operator=(const SegmentData& segData)
{
    if (this == &segData)
    {
        return *this;
    }
    SegmentDataBase::operator = (segData);
    mSegmentInfo = segData.mSegmentInfo;
    mSegmentMetrics = segData.mSegmentMetrics;
    mDirectory = segData.mDirectory;
    mSubSegmentData = segData.mSubSegmentData;
    ScopedLock lock(segData.mLock);
    mIndexDirectory = segData.mIndexDirectory;
    mAttrDirectory = segData.mAttrDirectory;
    mSummaryDirectory = segData.mSummaryDirectory;
    mPatchAccessor = segData.mPatchAccessor;
    mSegmentFileMeta = segData.mSegmentFileMeta;
    return *this;
}

string SegmentData::GetShardingDirInSegment(uint32_t shardingIdx) const
{
    return SHARDING_DIR_NAME_PREFIX + string("_") + StringUtil::toString<uint32_t>(shardingIdx);
}

SegmentData SegmentData::CreateShardingSegmentData(uint32_t shardingIdx) const
{
    assert(!mSubSegmentData);

    if (mSegmentInfo.shardingColumnCount == 1)
    {
        // No sharding
        SegmentData segmentData(*this);
        segmentData.mSegmentInfo.shardingColumnId = 0;
        return segmentData;
    }

    assert(shardingIdx < mSegmentInfo.shardingColumnCount);

    if (mSegmentInfo.shardingColumnId != SegmentInfo::INVALID_SHARDING_ID)
    {
        // segment is a sharding
        if (shardingIdx != mSegmentInfo.shardingColumnId)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "get shardingColumn[%u] difference from column[%u / %u]in segment[%u]",
                    shardingIdx, mSegmentInfo.shardingColumnId,
                    mSegmentInfo.shardingColumnCount,mSegmentId);
        }
        return *this;
    }

    // sharding in segment
    SegmentData segmentData(*this);
    // TODO: segmentinfo.docCount, mBaseDocId is not correct
    segmentData.mSegmentInfo.shardingColumnId = shardingIdx;
    const string& shardingPath = GetShardingDirInSegment(shardingIdx);
    DirectoryPtr shardingDirectory = mDirectory->GetDirectory(shardingPath, true);
    segmentData.SetDirectory(shardingDirectory);
    return segmentData;
}

IE_NAMESPACE_END(index_base);

