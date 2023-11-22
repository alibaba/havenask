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
#pragma once

#include <memory>

#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/patch/patch_file_info.h"
#include "indexlib/index_base/segment/segment_data.h"

namespace indexlib { namespace index {

template <typename T>
class UniqEncodeVarNumAttributeSegmentReaderForOffline : public AttributeSegmentReader
{
public:
    UniqEncodeVarNumAttributeSegmentReaderForOffline(const config::AttributeConfigPtr& config) : mAttrConfig(config) {}
    ~UniqEncodeVarNumAttributeSegmentReaderForOffline()
    {
        mReadCtx.reset();
        DELETE_AND_SET_NULL(mDupPool);
        DELETE_AND_SET_NULL(mReadPool);
    }

public:
    void Open(const index_base::PartitionDataPtr& partitionData, const index_base::SegmentInfo& segInfo,
              segmentid_t segmentId);
    ReadContextBasePtr CreateReadContextPtr(autil::mem_pool::Pool*) const override { return nullptr; }
    bool ReadDataAndLen(docid_t docId, const AttributeSegmentReader::ReadContextBasePtr&, uint8_t* buf, uint32_t bufLen,
                        uint32_t& dataLen) override;
    bool IsInMemory() const override { return false; }
    uint32_t GetMaxDataItemLen() const;
    bool Read(docid_t docId, const AttributeSegmentReader::ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen,
              bool& isNull) override
    {
        assert(false);
        return false;
    }
    uint32_t TEST_GetDataLength(docid_t docId) const override
    {
        assert(false);
        return 0;
    }
    uint64_t GetOffset(docid_t docId, const ReadContextBasePtr& ctx) const override
    {
        EnsureReadCtx();
        typedef typename MultiValueAttributeSegmentReader<T>::ReadContext ReadContext;
        ReadContext* typedCtx = (ReadContext*)mReadCtx.get();
        return typedCtx->offsetReader.GetOffset(docId);
    }
    bool Updatable() const override
    {
        assert(false);
        return false;
    }

private:
    file_system::DirectoryPtr GetAttributeDirectory(const index_base::PartitionDataPtr& partitionData,
                                                    segmentid_t segmentId) const;
    void CreatePatchReader(const index_base::PartitionDataPtr& partitionData, segmentid_t segmentId);
    void EnsureReadCtx() const;

private:
    typedef MultiValueAttributeSegmentReader<T> DFSReader;
    typedef std::shared_ptr<DFSReader> DFSReaderPtr;
    config::AttributeConfigPtr mAttrConfig;
    segmentid_t mSegmentId;
    DFSReaderPtr mSegmentReader;
    AttributePatchReaderPtr mPatchReader;
    std::set<uint64_t> mDupOffsets;
    std::map<uint64_t, autil::StringView> mDupValues;
    autil::mem_pool::Pool* mDupPool;
    autil::mem_pool::Pool* mReadPool;
    mutable AttributeSegmentReader::ReadContextBasePtr mReadCtx;
    size_t mSwitchLimit;

private:
    friend class UniqEncodeVarNumAttributeSegmentReaderForOfflineTest;

private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE(index, UniqEncodeVarNumAttributeSegmentReaderForOffline);
template <typename T>
inline file_system::DirectoryPtr UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::GetAttributeDirectory(
    const index_base::PartitionDataPtr& partitionData, segmentid_t segmentId) const
{
    assert(partitionData);

    index_base::SegmentData segData = partitionData->GetSegmentData(segmentId);

    const std::string& attrName = mAttrConfig->GetAttrName();
    file_system::DirectoryPtr directory;
    config::AttributeConfig::ConfigType configType = mAttrConfig->GetConfigType();
    if (configType == config::AttributeConfig::ct_section) {
        std::string indexName = config::SectionAttributeConfig::SectionAttributeNameToIndexName(attrName);
        directory = segData.GetSectionAttributeDirectory(indexName, true);
    } else {
        directory = segData.GetAttributeDirectory(attrName, true);
    }
    return directory;
}

template <typename T>
inline void UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::EnsureReadCtx() const
{
    if (mReadPool->getUsedBytes() >= mSwitchLimit) {
        IE_LOG(WARN, "switch ctx, may be need expend chunk memory size (if this log often happened, set env "
                     "MERGE_SWITCH_MEMORY_LIMIT)");
        mReadCtx.reset();
        mReadPool->release();
    }
    if (!mReadCtx) {
        mReadCtx = mSegmentReader->CreateReadContextPtr(mReadPool);
    }
}

template <typename T>
inline void UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::Open(const index_base::PartitionDataPtr& partitionData,
                                                                      const index_base::SegmentInfo& segInfo,
                                                                      segmentid_t segmentId)
{
    assert(segInfo.docCount > 0);
    mReadPool = new autil::mem_pool::Pool();
    mSwitchLimit = autil::EnvUtil::getEnv("MERGE_SWITCH_MEMORY_LIMIT", 100 * 1024 * 1024);
    // default 10M
    file_system::DirectoryPtr directory = GetAttributeDirectory(partitionData, segmentId);
    assert(directory);
    std::unique_ptr<MultiValueAttributeSegmentReader<T>> onDiskReader(
        new MultiValueAttributeSegmentReader<T>(mAttrConfig));
    index_base::SegmentData segData = partitionData->GetSegmentData(segmentId);
    onDiskReader->Open(segData, PatchApplyOption::NoPatch(), directory, nullptr, true);
    assert(onDiskReader->GetBaseAddress() == nullptr);
    mSegmentReader.reset(onDiskReader.release());
    EnsureReadCtx();
    CreatePatchReader(partitionData, segmentId);
    uint64_t docCount = segInfo.docCount;
    std::set<uint64_t> offsets;
    for (size_t docId = 0; docId < docCount; docId++) {
        uint64_t offset = GetOffset(docId, nullptr);
        if (offsets.find(offset) != offsets.end()) {
            mDupOffsets.insert(offset);
        } else {
            offsets.insert(offset);
        }
    }
    mDupPool = new autil::mem_pool::Pool();
}

template <typename T>
inline void UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::CreatePatchReader(
    const index_base::PartitionDataPtr& partitionData, segmentid_t segmentId)
{
    index_base::PatchFileFinderPtr patchFinder = index_base::PatchFileFinderCreator::Create(partitionData.get());

    index_base::PatchFileInfoVec patchVec;
    patchFinder->FindAttrPatchFilesForTargetSegment(mAttrConfig, segmentId, &patchVec);

    mPatchReader.reset(new VarNumAttributePatchReader<T>(mAttrConfig));
    for (size_t i = 0; i < patchVec.size(); i++) {
        mPatchReader->AddPatchFile(patchVec[i].patchDirectory, patchVec[i].patchFileName, patchVec[i].srcSegment);
    }
}

template <typename T>
inline bool UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::ReadDataAndLen(
    docid_t docId, const AttributeSegmentReader::ReadContextBasePtr&, uint8_t* buf, uint32_t bufLen, uint32_t& dataLen)
{
    EnsureReadCtx();
    uint64_t offset = GetOffset(docId, nullptr);
    if (mDupOffsets.find(offset) != mDupOffsets.end()) {
        auto iter = mDupValues.find(offset);
        if (iter == mDupValues.end()) {
            if (!this->mSegmentReader->ReadDataAndLen(docId, mReadCtx, buf, bufLen, dataLen)) {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, "read attribute data for doc [%d] failed.", docId);
                return false;
            }
            char* newBuf = (char*)mDupPool->allocate(dataLen);
            memcpy(newBuf, buf, dataLen);
            autil::StringView value(newBuf, dataLen);
            mDupValues.insert(std::make_pair(offset, value));
        } else {
            dataLen = iter->second.size();
            memcpy(buf, iter->second.data(), iter->second.size());
        }
    } else {
        if (!this->mSegmentReader->ReadDataAndLen(docId, mReadCtx, buf, bufLen, dataLen)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "read attribute data for doc [%d] failed.", docId);
            return false;
        }
    }
    uint32_t ret = this->mPatchReader->Seek(docId, buf, bufLen);
    if (ret > 0) {
        dataLen = ret;
        return true;
    }
    return true;
}

template <typename T>
inline uint32_t UniqEncodeVarNumAttributeSegmentReaderForOffline<T>::GetMaxDataItemLen() const
{
    uint32_t maxItemLen = 0;
    maxItemLen = std::max(mPatchReader->GetMaxPatchItemLen(), maxItemLen);
    maxItemLen = std::max(mSegmentReader->GetMaxDataItemLen(), maxItemLen);
    return maxItemLen;
}
}} // namespace indexlib::index
