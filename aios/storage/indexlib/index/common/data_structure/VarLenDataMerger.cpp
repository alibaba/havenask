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
#include "indexlib/index/common/data_structure/VarLenDataMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/merger/DocumentMergeInfo.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataMerger);

VarLenDataMerger::VarLenDataMerger(const VarLenDataParam& param) : _param(param), _pool(DEFAULT_POOL_BUFFER_SIZE) {}

VarLenDataMerger::~VarLenDataMerger() {}

void VarLenDataMerger::Init(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                            const std::shared_ptr<DocMapper>& docMapper, const std::vector<InputData>& inputReaders,
                            const std::vector<OutputData>& outputWriters)
{
    _segmentMergeInfos = segMergeInfos;
    assert(inputReaders.size() == _segmentMergeInfos.srcSegments.size());
    assert(outputWriters.size() == _segmentMergeInfos.targetSegments.size());
    _heap.Init(_segmentMergeInfos, docMapper);

    std::vector<std::pair<OutputData, segmentid_t>> outputInfos;
    outputInfos.reserve(outputWriters.size());
    for (auto output : outputWriters) {
        outputInfos.push_back(std::make_pair(output, output.targetSegmentId));
    }
    _segOutputMapper.Init(docMapper, outputInfos);
    _inputDatas = inputReaders;
    _outputDatas = outputWriters;
}

size_t VarLenDataMerger::EstimateMemoryUse(uint32_t docCount)
{
    size_t size = docCount * sizeof(uint64_t)                                 // offset
                  + indexlib::file_system::ReaderOption::DEFAULT_BUFFER_SIZE; // data buffer
    if (_param.dataItemUniqEncode) {
        size += docCount * sizeof(OffsetPair);
        size += docCount * sizeof(uint64_t); // offsetMap
    }
    return size;
}

Status VarLenDataMerger::Merge()
{
    Status status;
    if (_param.dataItemUniqEncode) {
        status = UniqMerge();
    } else {
        status = NormalMerge();
    }
    auto closeStatus = CloseOutputDatas();
    RETURN_IF_STATUS_ERROR(status, "do merge operation fail");
    RETURN_IF_STATUS_ERROR(closeStatus, "close output data writer fail");
    return Status::OK();
}

Status VarLenDataMerger::NormalMerge()
{
    std::vector<char> patchValueBuffer;
    DocumentMergeInfo info;
    while (_heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        auto currentBaseDocId = _segmentMergeInfos.srcSegments[segIdx].baseDocid;
        docid_t currentLocalId = info.oldDocId - currentBaseDocId;
        auto output = _segOutputMapper.GetOutput(info.oldDocId);
        if (!output) {
            continue;
        }

        autil::StringView data;
        if (_inputDatas[segIdx].segPatchReader) {
            uint32_t maxPatchItemLen = _inputDatas[segIdx].segPatchReader->GetMaxPatchItemLen();
            if (patchValueBuffer.size() < maxPatchItemLen) {
                patchValueBuffer.resize(maxPatchItemLen);
            }
            bool isNull = false;
            auto [status, dataLen] = _inputDatas[segIdx].segPatchReader->Seek(
                currentLocalId, (uint8_t*)patchValueBuffer.data(), patchValueBuffer.size(), isNull);
            RETURN_IF_STATUS_ERROR(status, "read data from patch fail for doc [%d]", currentLocalId);
            if (dataLen > 0) {
                data = autil::StringView(patchValueBuffer.data(), dataLen);
            }
        }
        if (data.length() == 0) {
            if (nullptr == _inputDatas[segIdx].dataReader) {
                RETURN_STATUS_ERROR(InternalError, "can not find data reader for doc [%d] in segment [%d]",
                                    info.oldDocId, segIdx);
            }
            auto [status, ret] = _inputDatas[segIdx].dataReader->GetValue(currentLocalId, data, &_pool);
            if (!ret) {
                RETURN_STATUS_ERROR(InternalError, "read data for doc [%d] failed.", info.oldDocId);
            }
        }
        RETURN_IF_STATUS_ERROR(output->dataWriter->AppendValue(data),
                               "append data into output data writer fail for doc [%d]", info.oldDocId);
        ResetPoolBuffer();
    }
    return Status::OK();
}

Status VarLenDataMerger::UniqMerge()
{
    RETURN_IF_STATUS_ERROR(ReserveMergedOffsets(), "reserve merge offset fail");
    auto docMapper = _heap.GetReclaimMap();
    OffsetMapVec offsetMapVec(_inputDatas.size());
    for (size_t i = 0; i < _inputDatas.size(); ++i) {
        if (nullptr == _inputDatas[i].dataReader) {
            continue;
        }
        RETURN_IF_STATUS_ERROR(ConstructSegmentOffsetMap(_segmentMergeInfos.srcSegments[i], docMapper,
                                                         _inputDatas[i].dataReader, offsetMapVec[i]),
                               "construct offset map fail for segment [%d]",
                               _segmentMergeInfos.srcSegments[i].segment->GetSegmentId());
        auto status = MergeOneSegmentData(_inputDatas[i].dataReader, offsetMapVec[i], docMapper,
                                          _segmentMergeInfos.srcSegments[i]);
        RETURN_IF_STATUS_ERROR(status, "fail to merge segment [%d]",
                               _segmentMergeInfos.srcSegments[i].segment->GetSegmentId());
    }
    RETURN_IF_STATUS_ERROR(MergeDocOffsets(docMapper, offsetMapVec), "merge data offset fail");
    return Status::OK();
}

Status VarLenDataMerger::ConstructSegmentOffsetMap(const IIndexMerger::SourceSegment& sourceSegmentInfo,
                                                   const std::shared_ptr<DocMapper>& docMapper,
                                                   const std::shared_ptr<VarLenDataReader>& segReader,
                                                   SegmentOffsetMap& segmentOffsetMap)
{
    auto segmentInfo = sourceSegmentInfo.segment->GetSegmentInfo();
    assert(segmentInfo);
    uint32_t docCount = segmentInfo->docCount;
    segmentOffsetMap.reserve(docCount);
    docid_t baseDocId = sourceSegmentInfo.baseDocid;
    for (docid_t oldDocId = 0; oldDocId < (int64_t)docCount; oldDocId++) {
        docid_t globalDocId = baseDocId + oldDocId;
        auto newDocId = docMapper->GetNewId(globalDocId);
        if (newDocId == INVALID_DOCID || _patchDocIdSet.count(globalDocId)) {
            continue;
        }
        auto newLocalInfo = docMapper->Map(globalDocId);
        auto output = _segOutputMapper.GetOutputBySegId(newLocalInfo.first);
        if (!output) {
            continue;
        }
        auto [status, oldOffset] = segReader->GetOffset(oldDocId);
        RETURN_IF_STATUS_ERROR(status, "get data offset fail for doc [%d]", oldDocId);
        segmentOffsetMap.push_back(OffsetPair(oldOffset, uint64_t(-1), oldDocId, newLocalInfo.first));
    }
    std::sort(segmentOffsetMap.begin(), segmentOffsetMap.end());
    segmentOffsetMap.assign(segmentOffsetMap.begin(), std::unique(segmentOffsetMap.begin(), segmentOffsetMap.end()));
    return Status::OK();
}

Status VarLenDataMerger::MergeOneSegmentData(const std::shared_ptr<VarLenDataReader>& reader,
                                             SegmentOffsetMap& segmentOffsetMap,
                                             const std::shared_ptr<DocMapper>& docMapper,
                                             const IIndexMerger::SourceSegment& sourceSegmentInfo)
{
    docid_t oldBaseDocId = sourceSegmentInfo.baseDocid;
    typename SegmentOffsetMap::iterator it = segmentOffsetMap.begin();
    for (; it != segmentOffsetMap.end(); it++) {
        auto oldGlobalDocId = oldBaseDocId + it->oldDocId;
        auto output = _segOutputMapper.GetOutput(oldGlobalDocId);
        assert(output);
        autil::StringView data;
        auto [status, ret] = reader->GetValue(it->oldDocId, data, &_pool);
        RETURN_IF_STATUS_ERROR(status, "fail to get data from doc id [%d]", it->oldDocId);
        if (!ret) {
            RETURN_STATUS_ERROR(InternalError, "read data for doc [%d] in segment [%d] failed.", it->oldDocId,
                                sourceSegmentInfo.segment->GetSegmentId());
        }
        auto [offsetStatus, newOffset] = FindOrGenerateNewOffset(data, *output);
        RETURN_IF_STATUS_ERROR(offsetStatus, "gen new offset fail for doc [%d]", oldGlobalDocId);
        it->newOffset = newOffset;
        ResetPoolBuffer();
    }
    return Status::OK();
}

Status VarLenDataMerger::MergeDocOffsets(const std::shared_ptr<DocMapper>& docMapper, const OffsetMapVec& offsetMapVec)
{
    DocumentMergeInfo info;
    while (_heap.GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        auto baseDocId = _segmentMergeInfos.srcSegments[segIdx].baseDocid;
        docid_t oldDocId = info.oldDocId - baseDocId;
        docid_t globalDocId = info.oldDocId;
        if (_patchDocIdSet.count(globalDocId) > 0) {
            continue;
        }
        auto newLocalInfo = docMapper->Map(globalDocId);
        auto output = _segOutputMapper.GetOutputBySegId(newLocalInfo.first);
        if (!output) {
            continue;
        }
        if (nullptr == _inputDatas[segIdx].dataReader) {
            RETURN_STATUS_ERROR(InternalError, "can not find data reader for doc [%d] in segment [%d]", oldDocId,
                                segIdx);
        }
        const std::shared_ptr<VarLenDataReader>& reader = _inputDatas[segIdx].dataReader;
        auto [status, oldOffset] = reader->GetOffset(oldDocId);
        RETURN_IF_STATUS_ERROR(status, "reader data offset fail for doc [%d]", oldDocId);
        const SegmentOffsetMap& offsetMap = offsetMapVec[segIdx];
        typename SegmentOffsetMap::const_iterator it = std::lower_bound(
            offsetMap.begin(), offsetMap.end(), OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        assert(it != offsetMap.end() && *it == OffsetPair(oldOffset, uint64_t(-1), INVALID_DOCID, newLocalInfo.first));
        output->dataWriter->SetOffset(newLocalInfo.second, it->newOffset);
    }
    return Status::OK();
}

void VarLenDataMerger::ResetPoolBuffer()
{
    if (_pool.getTotalBytes() > DEFAULT_POOL_BUFFER_SIZE) {
        _pool.release();
    } else {
        _pool.reset();
    }
}

std::pair<Status, uint64_t> VarLenDataMerger::FindOrGenerateNewOffset(const autil::StringView& value,
                                                                      OutputData& output)
{
    uint64_t hashValue = output.dataWriter->GetHashValue(value);
    return output.dataWriter->AppendValueWithoutOffset(value, hashValue);
}

Status VarLenDataMerger::ReserveMergedOffsets()
{
    std::vector<char> patchValueBuffer;
    std::shared_ptr<DocumentMergeInfoHeap> heap(_heap.Clone());
    DocumentMergeInfo info;
    while (heap->GetNext(info)) {
        int32_t segIdx = info.segmentIndex;
        auto currentBaseDocId = _segmentMergeInfos.srcSegments[segIdx].baseDocid;
        docid_t currentLocalId = info.oldDocId - currentBaseDocId;
        auto output = _segOutputMapper.GetOutput(info.oldDocId);
        if (!output) {
            continue;
        }

        const auto& patchReader = _inputDatas[segIdx].segPatchReader;
        if (patchReader) {
            uint32_t maxPatchItemLen = patchReader->GetMaxPatchItemLen();
            if (patchValueBuffer.size() < maxPatchItemLen) {
                patchValueBuffer.resize(maxPatchItemLen);
            }
            bool isNull = false;
            auto [status, dataLen] =
                patchReader->Seek(currentLocalId, (uint8_t*)patchValueBuffer.data(), patchValueBuffer.size(), isNull);
            RETURN_IF_STATUS_ERROR(status, "read data from patch fail for doc [%d]", currentLocalId);
            if (dataLen > 0) {
                autil::StringView value((const char*)patchValueBuffer.data(), dataLen);
                RETURN_IF_STATUS_ERROR(output->dataWriter->AppendValue(value),
                                       "append new value into data writer fail for doc [%d]", currentLocalId);
                docid_t globalDocId = _segmentMergeInfos.srcSegments[segIdx].baseDocid + currentLocalId;
                _patchDocIdSet.insert(globalDocId);
                continue;
            }
        }
        // delay merge data. Index: merge order, Value: [newDocId --> DocumentMergeInfo]
        output->dataWriter->AppendOffset(0);
    }
    return Status::OK();
}

Status VarLenDataMerger::CloseOutputDatas()
{
    for (size_t i = 0; i < _outputDatas.size(); i++) {
        RETURN_IF_STATUS_ERROR(_outputDatas[i].dataWriter->Close(), "close data writer fail in merge operation");
    }
    return Status::OK();
}

} // namespace indexlibv2::index
