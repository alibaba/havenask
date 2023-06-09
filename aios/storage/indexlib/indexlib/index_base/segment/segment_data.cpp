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
#include "indexlib/index_base/segment/segment_data.h"

#include "indexlib/config/section_attribute_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SegmentData);

const std::string SegmentData::LIFECYCLE_IN_MEM = "__IN_MEM__";

SegmentData::SegmentData()
    : SegmentDataBase()
    , mSegmentInfo(new SegmentInfo)
    , mSegmentMetrics(new indexlib::framework::SegmentMetrics)
{
}

SegmentData::SegmentData(const SegmentData& segData) { *this = segData; }

SegmentData::~SegmentData() {}

DirectoryPtr SegmentData::GetIndexDirectory(const string& indexName, bool throwExceptionIfNotExist) const
{
    DirectoryPtr indexDirectory;
    if (mPatchAccessor) {
        indexDirectory = mPatchAccessor->GetIndexDirectory(mSegmentId, indexName, throwExceptionIfNotExist);
        if (indexDirectory) {
            return indexDirectory;
        }
    }

    indexDirectory = GetIndexDirectory(throwExceptionIfNotExist);
    if (!indexDirectory) {
        return DirectoryPtr();
    }
    return indexDirectory->GetDirectory(indexName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetSectionAttributeDirectory(const string& indexName, bool throwExceptionIfNotExist) const
{
    DirectoryPtr sectionAttrDir;
    if (mPatchAccessor) {
        sectionAttrDir = mPatchAccessor->GetSectionAttributeDirectory(mSegmentId, indexName, throwExceptionIfNotExist);
        if (sectionAttrDir) {
            return sectionAttrDir;
        }
    }

    DirectoryPtr indexDirectory = GetIndexDirectory(throwExceptionIfNotExist);
    if (!indexDirectory) {
        return DirectoryPtr();
    }
    string sectionAttrName = SectionAttributeConfig::IndexNameToSectionAttributeName(indexName);
    return indexDirectory->GetDirectory(sectionAttrName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetAttributeDirectory(const std::string& attrName, bool throwExceptionIfNotExist) const
{
    DirectoryPtr attrDirectory;
    if (mPatchAccessor) {
        attrDirectory = mPatchAccessor->GetAttributeDirectory(mSegmentId, attrName, throwExceptionIfNotExist);
        if (attrDirectory) {
            return attrDirectory;
        }
    }
    attrDirectory = GetAttributeDirectory(throwExceptionIfNotExist);
    if (!attrDirectory) {
        return DirectoryPtr();
    }
    return attrDirectory->GetDirectory(attrName, throwExceptionIfNotExist);
}

DirectoryPtr SegmentData::GetOperationDirectory(bool throwExceptionIfNotExist) const
{
    ScopedLock lock(mLock);

    if (unlikely(!mOperationDirectory)) {
        assert(mDirectory);
        mOperationDirectory = mDirectory->GetDirectory(OPERATION_DIR_NAME, throwExceptionIfNotExist);
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
    if (unlikely(!mIndexDirectory)) {
        assert(mDirectory);
        mIndexDirectory = mDirectory->GetDirectory(INDEX_DIR_NAME, throwExceptionIfNotExist);
    }
    return mIndexDirectory;
}

DirectoryPtr SegmentData::GetAttributeDirectory(bool throwExceptionIfNotExist) const
{
    ScopedLock lock(mLock);
    if (unlikely(!mAttrDirectory)) {
        assert(mDirectory);
        mAttrDirectory = mDirectory->GetDirectory(ATTRIBUTE_DIR_NAME, throwExceptionIfNotExist);
    }
    return mAttrDirectory;
}

DirectoryPtr SegmentData::GetSummaryDirectory(bool throwExceptionIfNotExist) const
{
    ScopedLock lock(mLock);
    if (unlikely(!mSummaryDirectory)) {
        assert(mDirectory);
        mSummaryDirectory = mDirectory->GetDirectory(SUMMARY_DIR_NAME, throwExceptionIfNotExist);
    }
    return mSummaryDirectory;
}

DirectoryPtr SegmentData::GetSourceDirectory(bool throwExceptionIfNotExist) const
{
    ScopedLock lock(mLock);
    if (unlikely(!mSourceDirectory)) {
        assert(mDirectory);
        mSourceDirectory = mDirectory->GetDirectory(SOURCE_DIR_NAME, throwExceptionIfNotExist);
    }
    return mSourceDirectory;
}

SegmentData& SegmentData::operator=(const SegmentData& segData)
{
    if (this == &segData) {
        return *this;
    }
    SegmentDataBase::operator=(segData);
    mSegmentInfo = segData.mSegmentInfo;
    mSegmentMetrics = segData.mSegmentMetrics;
    mDirectory = segData.mDirectory;
    mSubSegmentData = segData.mSubSegmentData;
    ScopedLock lock(segData.mLock);
    mIndexDirectory = segData.mIndexDirectory;
    mAttrDirectory = segData.mAttrDirectory;
    mSummaryDirectory = segData.mSummaryDirectory;
    mPatchAccessor = segData.mPatchAccessor;
    mLifecycle = segData.mLifecycle;
    return *this;
}

string SegmentData::GetShardingDirInSegment(uint32_t shardingIdx) const
{
    return SHARDING_DIR_NAME_PREFIX + string("_") + StringUtil::toString<uint32_t>(shardingIdx);
}

SegmentData SegmentData::CreateShardingSegmentData(uint32_t shardingIdx) const
{
    assert(!mSubSegmentData);

    if (mSegmentInfo->shardCount == 1) {
        // No sharding
        SegmentData segmentData(*this);
        SegmentInfo segmentInfo(*mSegmentInfo);
        segmentInfo.shardId = 0;
        segmentData.mSegmentInfo.reset(new SegmentInfo(move(segmentInfo)));
        return segmentData;
    }

    assert(shardingIdx < mSegmentInfo->shardCount);

    if (mSegmentInfo->shardId != SegmentInfo::INVALID_SHARDING_ID) {
        // segment is a sharding
        if (shardingIdx != mSegmentInfo->shardId) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "get shardingColumn[%u] difference from column[%u / %u]in segment[%u]", shardingIdx,
                                 mSegmentInfo->shardId, mSegmentInfo->shardCount, mSegmentId);
        }
        return *this;
    }

    // sharding in segment
    SegmentData segmentData(*this);
    SegmentInfo segmentInfo(*mSegmentInfo);
    segmentInfo.shardId = shardingIdx;
    segmentData.mSegmentInfo.reset(new SegmentInfo(move(segmentInfo)));
    // TODO: segmentinfo.docCount, mBaseDocId is not correct
    const string& shardingPath = GetShardingDirInSegment(shardingIdx);
    DirectoryPtr shardingDirectory = mDirectory->GetDirectory(shardingPath, true);
    segmentData.SetDirectory(shardingDirectory);
    return segmentData;
}

string SegmentData::GetOriginalTemperature() const
{
    SegmentTemperatureMeta meta;
    if (!mSegmentInfo->GetOriginalTemperatureMeta(meta)) {
        return string("");
    }
    return meta.segTemperature;
}

}} // namespace indexlib::index_base
