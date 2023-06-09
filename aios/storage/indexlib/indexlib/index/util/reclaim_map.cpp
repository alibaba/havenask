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
#include "indexlib/index/util/reclaim_map.h"

#include "autil/LongHashValue.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, ReclaimMap);

const std::string ReclaimMap::RECLAIM_MAP_FILE_NAME = "reclaim_map";

ReclaimMap::ReclaimMap() : mDeletedDocCount(0), mNewDocId(0) {}
ReclaimMap::ReclaimMap(uint32_t deletedDocCount, docid_t newDocId, std::vector<docid_t>&& docIdArray,
                       std::vector<docid_t>&& joinValueArray, std::vector<docid_t>&& targetBaseDocIds)
    : mDeletedDocCount(deletedDocCount)
    , mNewDocId(newDocId)
    , mDocIdArray(docIdArray)
    , mJoinValueArray(joinValueArray)
    , mTargetBaseDocIds(targetBaseDocIds)
    , mDocIdOrder(nullptr)
    , mReverseGlobalIdArray(nullptr)
{
}

ReclaimMap::ReclaimMap(const ReclaimMap& other)
    : mDeletedDocCount(other.mDeletedDocCount)
    , mNewDocId(other.mNewDocId)
    , mDocIdArray(other.mDocIdArray)
{
    if (other.mDocIdOrder) {
        mDocIdOrder.reset(new DocOrder(*other.mDocIdOrder));
    }
    if (other.mReverseGlobalIdArray) {
        mReverseGlobalIdArray.reset(new GlobalIdVector(*(other.mReverseGlobalIdArray)));
    }
}

ReclaimMap::~ReclaimMap() {}

void ReclaimMap::Init(uint32_t docCount)
{
    mDeletedDocCount = 0;
    mDocIdArray.resize(docCount);
}

ReclaimMap* ReclaimMap::Clone() const { return new ReclaimMap(*this); }

void ReclaimMap::InitReverseDocIdArray(const SegmentMergeInfos& segMergeInfos)
{
    if (mNewDocId == 0) {
        return;
    }

    if (HasReverseDocIdArray()) {
        return;
    }

    mReverseGlobalIdArray.reset(new GlobalIdVector);
    mReverseGlobalIdArray->Resize(mNewDocId);

    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        uint32_t docCount = segMergeInfos[i].segmentInfo.docCount;
        docid_t baseDocId = segMergeInfos[i].baseDocId;
        for (uint32_t j = 0; j < docCount; j++) {
            docid_t newDocId = mDocIdArray[baseDocId + j];
            if (newDocId != INVALID_DOCID) {
                GlobalId gid = make_pair(segMergeInfos[i].segmentId, j);
                (*mReverseGlobalIdArray)[newDocId] = gid;
            }
        }
    }
}

template <typename T, typename SizeType>
bool ReclaimMap::LoadVector(FileReaderPtr& reader, vector<T>& vec)
{
    SizeType size = 0;
    if (reader->Read(&size, sizeof(size)).GetOrThrow() != sizeof(size)) {
        IE_LOG(ERROR, "read vector size failed");
        return false;
    }
    if (size > 0) {
        vec.resize(size);
        int64_t expectedSize = sizeof(vec[0]) * size;
        if (reader->Read(vec.data(), expectedSize).GetOrThrow() != (size_t)expectedSize) {
            IE_LOG(ERROR, "read vector data failed");
            return false;
        }
    }
    return true;
}

template <typename T, typename SizeType>
void ReclaimMap::StoreVector(const FileWriterPtr& writer, const std::vector<T>& vec) const
{
    SizeType size = static_cast<SizeType>(vec.size());
    writer->Write(&size, sizeof(size)).GetOrThrow();
    if (size > 0) {
        writer->Write(vec.data(), sizeof(vec[0]) * size).GetOrThrow();
    }
}

void ReclaimMap::StoreForMerge(const std::string& filePath, const SegmentMergeInfos& segMergeInfos) const
{
    BufferedFileWriterPtr bufferedFileWriter(new BufferedFileWriter);
    THROW_IF_FS_ERROR(bufferedFileWriter->Open(filePath, filePath), "BufferedFileWriter open failed, path[%s]",
                      filePath.c_str());
    InnerStore(bufferedFileWriter, segMergeInfos);
}

void ReclaimMap::StoreForMerge(const DirectoryPtr& directory, const string& fileName,
                               const SegmentMergeInfos& segMergeInfos) const
{
    FileWriterPtr writer = directory->CreateFileWriter(fileName);
    InnerStore(writer, segMergeInfos);
}

void ReclaimMap::InnerStore(const FileWriterPtr& writer, const SegmentMergeInfos& segMergeInfos) const
{
    writer->Write(&mDeletedDocCount, sizeof(mDeletedDocCount)).GetOrThrow();
    writer->Write(&mNewDocId, sizeof(mNewDocId)).GetOrThrow();
    uint32_t docCount = mDocIdArray.size();
    writer->Write(&docCount, sizeof(docCount)).GetOrThrow();
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        writer
            ->Write(mDocIdArray.data() + segMergeInfos[i].baseDocId,
                    sizeof(mDocIdArray[0]) * segMergeInfos[i].segmentInfo.docCount)
            .GetOrThrow();
    }
    uint32_t joinAttrCount = mJoinValueArray.size();
    assert(joinAttrCount == 0 || joinAttrCount == (uint32_t)mNewDocId);
    writer->Write(&joinAttrCount, sizeof(joinAttrCount)).GetOrThrow();
    if (joinAttrCount > 0) {
        writer->Write(mJoinValueArray.data(), sizeof(mJoinValueArray[0]) * joinAttrCount).GetOrThrow();
    }

    bool needReverseMapping = mReverseGlobalIdArray != NULL;
    writer->Write(&needReverseMapping, sizeof(needReverseMapping)).GetOrThrow();
    StoreVector(writer, mTargetBaseDocIds);

    bool needDocidOrder = mDocIdOrder != NULL;
    writer->Write(&needDocidOrder, sizeof(needDocidOrder)).GetOrThrow();
    if (needDocidOrder) {
        StoreVector(writer, *mDocIdOrder);
    }
    writer->Close().GetOrThrow();
}

bool ReclaimMap::LoadDocIdArray(FileReaderPtr& reader, const SegmentMergeInfos& segMergeInfos,
                                int64_t mergeMetaBinaryVersion)
{
    uint32_t docCount;
    size_t expectSize = sizeof(docCount);
    if (reader->Read(&docCount, expectSize).GetOrThrow() != expectSize) {
        IE_LOG(ERROR, "read doc count failed.");
        return false;
    }
    mDocIdArray.resize(docCount);

    if (mergeMetaBinaryVersion == -1) {
        expectSize = sizeof(mDocIdArray[0]) * docCount;
        if (reader->Read(&mDocIdArray[0], expectSize).GetOrThrow() != expectSize) {
            IE_LOG(ERROR, "read doc id array failed.");
            return false;
        }
    } else {
        for (size_t i = 0; i < segMergeInfos.size(); ++i) {
            expectSize = sizeof(mDocIdArray[0]) * segMergeInfos[i].segmentInfo.docCount;
            size_t actualSize = reader->Read(&mDocIdArray[0] + segMergeInfos[i].baseDocId, expectSize).GetOrThrow();
            if (actualSize != expectSize) {
                IE_LOG(ERROR,
                       "read docidarray[%u] failed. expect size [%lu], "
                       "actual size [%lu]",
                       (uint32_t)i, expectSize, actualSize);
                return false;
            }
        }
    }

    return true;
}

bool ReclaimMap::InnerLoad(FileReaderPtr& reader, const SegmentMergeInfos& segMergeInfos,
                           int64_t mergeMetaBinaryVersion)
{
    size_t expectSize = sizeof(mDeletedDocCount);
    if (reader->Read(&mDeletedDocCount, expectSize).GetOrThrow() != expectSize) {
        IE_LOG(ERROR, "read deleted doc count failed.");
        reader->Close().GetOrThrow();
        return false;
    }
    expectSize = sizeof(mNewDocId);
    if (reader->Read(&mNewDocId, expectSize).GetOrThrow() != expectSize) {
        IE_LOG(ERROR, "read new doc id failed.");
        reader->Close().GetOrThrow();
        return false;
    }
    if (!LoadDocIdArray(reader, segMergeInfos, mergeMetaBinaryVersion)) {
        reader->Close().GetOrThrow();
        return false;
    }

    uint32_t joinAttrCount;
    expectSize = sizeof(joinAttrCount);
    if (reader->Read(&joinAttrCount, expectSize).GetOrThrow() != expectSize) {
        IE_LOG(ERROR, "read join attr count failed.");
        reader->Close().GetOrThrow();
        return false;
    }

    mJoinValueArray.resize(joinAttrCount);
    if (joinAttrCount > 0) {
        expectSize = sizeof(mJoinValueArray[0]) * joinAttrCount;
        if (reader->Read(&mJoinValueArray[0], expectSize).GetOrThrow() != expectSize) {
            IE_LOG(ERROR, "read join value array failed.");
            reader->Close().GetOrThrow();
            return false;
        }
    }

    bool needReverseMapping = false;
    expectSize = sizeof(needReverseMapping);
    if (reader->Read(&needReverseMapping, expectSize).GetOrThrow() != expectSize) {
        IE_LOG(ERROR, "read needReverseMapping failed");
        reader->Close().GetOrThrow();
        return false;
    }

    if (needReverseMapping) {
        InitReverseDocIdArray(segMergeInfos);
    }

    if (mergeMetaBinaryVersion >= 2) {
        if (!LoadVector(reader, mTargetBaseDocIds)) {
            IE_LOG(ERROR, "load targetBaseDocIds failed");
            reader->Close().GetOrThrow();
            return false;
        }
    } else {
        mTargetBaseDocIds.clear();
    }

    if (mergeMetaBinaryVersion >= 3) {
        bool needDocOrder;
        expectSize = sizeof(needDocOrder);
        if (reader->Read(&needDocOrder, expectSize).GetOrThrow() != expectSize) {
            IE_LOG(ERROR, "read need doc order failed");
            reader->Close().GetOrThrow();
            return false;
        }
        if (needDocOrder) {
            mDocIdOrder.reset(new DocOrder());
            if (!LoadVector(reader, *mDocIdOrder)) {
                IE_LOG(ERROR, "read need doc order failed");
                reader->Close().GetOrThrow();
                return false;
            }
        }
    }
    reader->Close().GetOrThrow();
    return true;
}

bool ReclaimMap::LoadForMerge(const string& filePath, const SegmentMergeInfos& segMergeInfos,
                              int64_t mergeMetaBinaryVersion)
{
    BufferedFileReader* bufferedFileReader = new BufferedFileReader;
    THROW_IF_FS_ERROR(bufferedFileReader->Open(filePath), "file reader open failed");
    FileReaderPtr reader(bufferedFileReader);
    return InnerLoad(reader, segMergeInfos, mergeMetaBinaryVersion);
}

bool ReclaimMap::LoadForMerge(const DirectoryPtr& directory, const string& fileName,
                              const SegmentMergeInfos& segMergeInfos, int64_t mergeMetaBinaryVersion)
{
    FileReaderPtr reader = directory->CreateFileReader(fileName, FSOpenType::FSOT_BUFFERED);
    THROW_IF_FS_ERROR(reader->Open(), "file reader open failed");
    return InnerLoad(reader, segMergeInfos, mergeMetaBinaryVersion);
}

int64_t ReclaimMap::EstimateMemoryUse() const
{
    int64_t memUse = 0;
    memUse += mDocIdArray.capacity() * sizeof(docid_t);
    memUse += mJoinValueArray.capacity() * sizeof(docid_t);
    memUse += mTargetBaseDocIds.capacity() & sizeof(mTargetBaseDocIds[0]);

    if (mReverseGlobalIdArray) {
        memUse += mReverseGlobalIdArray->Capacity() * sizeof(GlobalId);
    }
    if (mDocIdOrder) {
        memUse += mDocIdOrder->capacity() * sizeof(docid_t);
    }
    return memUse;
}
}} // namespace indexlib::index
