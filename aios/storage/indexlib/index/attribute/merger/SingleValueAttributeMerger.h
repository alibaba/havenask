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
#include <algorithm>

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SliceInfo.h"
#include "indexlib/index/attribute/format/SingleValueAttributeFormatter.h"
#include "indexlib/index/attribute/format/SingleValueDataAppender.h"
#include "indexlib/index/attribute/merger/AttributeMerger.h"
#include "indexlib/index/attribute/merger/AttributeMergerCreator.h"
#include "indexlib/index/attribute/merger/DocumentMergeInfoHeap.h"
#include "indexlib/index/attribute/merger/SegmentOutputMapper.h"
#include "indexlib/index/attribute/patch/DedupPatchFileMerger.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeMerger : public AttributeMerger
{
public:
    SingleValueAttributeMerger() : AttributeMerger(), _recordSize(sizeof(T)) {}
    ~SingleValueAttributeMerger();

public:
    class Creator : public AttributeMergerCreator
    {
    public:
        FieldType GetAttributeType() const override { return TypeInfo<T>::GetFieldType(); }

        std::unique_ptr<AttributeMerger> Create(bool isUniqEncoded) const override
        {
            return std::make_unique<SingleValueAttributeMerger<T>>();
        }
    };

    using DiskIndexerWithCtx =
        std::pair<std::shared_ptr<AttributeDiskIndexer>, std::shared_ptr<AttributeDiskIndexer::ReadContextBase>>;

public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(single);
    Status DoMerge(const SegmentMergeInfos& segMergeInfos, std::shared_ptr<DocMapper>& docMapper) override;

private:
    struct OutputData {
        size_t outputIdx = 0;
        std::shared_ptr<AttributeFormatter> formatter;
        std::shared_ptr<SingleValueDataAppender> dataAppender;

        OutputData() = default;

        bool operator==(const OutputData& other) const { return outputIdx == other.outputIdx; }

        template <typename Type>
        void Set(docid_t globalDocId, const Type& value, bool isNull)
        {
            assert(dataAppender);
            assert(dataAppender->GetTotalCount() == (uint32_t)(globalDocId));
            dataAppender->Append(value, isNull);
        }

        bool BufferFull() const
        {
            assert(dataAppender);
            return dataAppender->IsFull();
        }

        void Flush()
        {
            assert(dataAppender);
            return dataAppender->Flush();
        }
    };

    std::shared_ptr<indexlib::file_system::FileWriter>
    CreateDataFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
                         const std::shared_ptr<framework::SegmentStatistics>& segmentStatistics);

    void PrepareCompressOutputData(const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegmentMetas);
    void FlushCompressDataBuffer(const OutputData& outputData);
    Status DumpCompressDataBuffer();

    void FlushDataBuffer(OutputData& outputData);

    Status PrepareOutputDatas(const std::shared_ptr<DocMapper>& docMapper,
                              const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegmentMetas);
    void DestroyBuffers();
    void CloseFiles();

    Status MergePatches(const SegmentMergeInfos segmentMergeInfos);
    Status CreateDiskIndexers(const SegmentMergeInfos& segMergeInfos,
                              std::vector<DiskIndexerWithCtx>& diskIndexersWithCtx);

    virtual std::pair<Status, std::shared_ptr<IIndexer>>
    GetDiskIndexer(const std::shared_ptr<framework::Segment>& segment);

    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir);

private:
    static const uint32_t DEFAULT_RECORD_COUNT = 1024 * 1024;
    uint32_t _recordSize;
    SegmentOutputMapper<OutputData> _segOutputMapper;
    std::vector<std::shared_ptr<EqualValueCompressDumper<T>>> _compressDumpers;
    std::shared_ptr<autil::mem_pool::PoolBase> _pool;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributeMerger, T);

template <typename T>
SingleValueAttributeMerger<T>::~SingleValueAttributeMerger()
{
    _compressDumpers.clear();
    _pool.reset();
}

template <typename T>
Status SingleValueAttributeMerger<T>::DoMerge(const SegmentMergeInfos& segMergeInfos,
                                              std::shared_ptr<DocMapper>& docMapper)
{
    SliceInfo sliceInfo(_attributeConfig->GetSliceCount(), _attributeConfig->GetSliceIdx());
    docid_t beginDocId = 0;
    docid_t endDocId = std::numeric_limits<docid_t>::max();
    if (_attributeConfig->GetSliceCount() > 1 && _attributeConfig->GetSliceIdx() >= 0) {
        if (segMergeInfos.targetSegments.size() != 1) {
            AUTIL_LOG(ERROR, "not support attribute slice with merge to multi segment");
            return Status::Corruption();
        }
        int64_t docCount = docMapper->GetTargetSegmentDocCount(0);
        sliceInfo.GetDocRange(docCount, beginDocId, endDocId);
    }
    std::vector<DiskIndexerWithCtx> diskIndexersWithCtx;
    RETURN_IF_STATUS_ERROR(CreateDiskIndexers(segMergeInfos, diskIndexersWithCtx), "create disk indexer failed");
    auto status = PrepareOutputDatas(docMapper, segMergeInfos.targetSegments);
    RETURN_IF_STATUS_ERROR(status, "prepare output data failed.");

    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, docMapper);
    DocumentMergeInfo info;
    while (heap.GetNext(info)) {
        auto output = _segOutputMapper.GetOutput(info.oldDocId);
        if (output == nullptr) {
            continue;
        }
        if (info.newDocId < beginDocId || info.newDocId > endDocId) {
            // not in attribute slice ignore
            continue;
        }
        int32_t segIdx = info.segmentIndex;
        docid_t curBaseDocId = segMergeInfos.srcSegments[segIdx].baseDocid;
        docid_t currentLocalId = info.oldDocId - curBaseDocId;

        T value {};
        bool isNull = false;
        auto [diskIndexer, ctx] = diskIndexersWithCtx[segIdx];
        uint32_t dataLen = 0;
        diskIndexer->Read(currentLocalId, ctx, (uint8_t*)&value, sizeof(value), dataLen, isNull);
        output->Set(info.newDocId - beginDocId, value, isNull);
        if (output->BufferFull()) {
            FlushDataBuffer(*output);
        }
    }

    for (auto& outputData : _segOutputMapper.GetOutputs()) {
        FlushDataBuffer(outputData);
    }

    if (AttributeCompressInfo::NeedCompressData(_attributeConfig)) {
        status = DumpCompressDataBuffer();
        RETURN_IF_STATUS_ERROR(status, "dump compress data buffer failed.");
    }

    CloseFiles();
    DestroyBuffers();

    status = MergePatches(segMergeInfos);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "merge patch failed.");
        return status;
    }
    return Status::OK();
}

template <typename T>
inline std::pair<Status, std::shared_ptr<IIndexer>>
SingleValueAttributeMerger<T>::GetDiskIndexer(const std::shared_ptr<framework::Segment>& segment)
{
    return segment->GetIndexer(_attributeConfig->GetIndexType(), _attributeConfig->GetIndexName());
}

template <typename T>
inline Status SingleValueAttributeMerger<T>::CreateDiskIndexers(const SegmentMergeInfos& segMergeInfos,
                                                                std::vector<DiskIndexerWithCtx>& diskIndexersWithCtx)
{
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        auto& segment = srcSegment.segment;
        if (segment->GetSegmentInfo()->docCount == 0) {
            diskIndexersWithCtx.emplace_back(std::make_pair(nullptr, nullptr));
            continue;
        }
        std::shared_ptr<AttributeDiskIndexer> diskIndexer;
        auto [status, indexer] = GetDiskIndexer(segment);
        if (!status.IsOK()) {
            auto segmentSchema = segment->GetSegmentSchema();
            if (status.IsNotFound() && segmentSchema &&
                segmentSchema->GetIndexConfig(ATTRIBUTE_INDEX_TYPE_STR, _attributeConfig->GetAttrName()) == nullptr) {
                auto [status, defaultIndexer] =
                    AttributeDefaultDiskIndexerFactory::CreateDefaultDiskIndexer(segment, _attributeConfig);
                RETURN_IF_STATUS_ERROR(status, "create default indexer for merge failed");
                diskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(defaultIndexer);
                assert(diskIndexer);
            } else {
                AUTIL_LOG(ERROR, "no attribute indexer for [%s] in segment [%d]",
                          _attributeConfig->GetIndexName().c_str(), segment->GetSegmentId());
                return Status::InternalError();
            }
        } else {
            diskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(indexer);
            assert(diskIndexer != nullptr);
        }
        diskIndexersWithCtx.emplace_back(std::make_pair(diskIndexer, diskIndexer->CreateReadContextPtr(nullptr)));
    }
    return Status::OK();
}

template <typename T>
Status SingleValueAttributeMerger<T>::PrepareOutputDatas(
    const std::shared_ptr<DocMapper>& docMapper,
    const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegmentMetas)
{
    auto createFunc = [this](const framework::SegmentMeta& segmentMeta, size_t outputIdx,
                             OutputData& output) -> Status {
        output.outputIdx = outputIdx;
        output.formatter = std::make_shared<SingleValueAttributeUpdatableFormatter<T>>(
            _attributeConfig->GetCompressType(), _attributeConfig->GetFieldConfig()->IsEnableNullField());

        assert(segmentMeta.segmentDir != nullptr);
        auto segDir = segmentMeta.segmentDir->GetIDirectory();
        assert(segDir != nullptr);
        auto [status, attrDir] = GetOutputDirectory(segDir);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make dir[attribute] failed.");
            return status;
        }

        output.dataAppender.reset(new SingleValueDataAppender(output.formatter.get()));

        assert(segmentMeta.segmentInfo);
        auto [statisticStatus, segmentStatistics] = segmentMeta.segmentInfo->GetSegmentStatistics();
        auto segmentStatisticsPtr = std::make_shared<framework::SegmentStatistics>(segmentStatistics);
        RETURN_IF_STATUS_ERROR(statisticStatus, "jsonize segment statistics failed");
        auto fileWriter = CreateDataFileWriter(attrDir, segmentStatisticsPtr);
        if (!fileWriter) {
            return Status::InternalError();
        }
        output.dataAppender->Init(DEFAULT_RECORD_COUNT, fileWriter);

        AUTIL_LOG(INFO, "create output data for dir [%s]", attrDir->DebugString().c_str());
        return status;
    };

    if (AttributeCompressInfo::NeedCompressData(_attributeConfig)) {
        PrepareCompressOutputData(targetSegmentMetas);
    }
    return _segOutputMapper.Init(docMapper, targetSegmentMetas, createFunc);
}

template <typename T>
std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
SingleValueAttributeMerger<T>::GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir)
{
    return segDir->MakeDirectory(index::ATTRIBUTE_INDEX_TYPE_STR, indexlib::file_system::DirectoryOption())
        .StatusWith();
}

template <typename T>
void SingleValueAttributeMerger<T>::PrepareCompressOutputData(
    const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegmentMetas)
{
    assert(_compressDumpers.empty());
    assert(!_pool.get());
    _pool.reset(new indexlib::util::SimplePool);
    while (_compressDumpers.size() < targetSegmentMetas.size()) {
        _compressDumpers.emplace_back(new EqualValueCompressDumper<T>(false, _pool.get()));
    }
}

template <typename T>
std::shared_ptr<indexlib::file_system::FileWriter> SingleValueAttributeMerger<T>::CreateDataFileWriter(
    const std::shared_ptr<indexlib::file_system::IDirectory>& attributeDir,
    const std::shared_ptr<framework::SegmentStatistics>& segmentStatistics)
{
    std::string attrPath = _attributeConfig->GetAttrName() + "/" + _attributeConfig->GetSliceDir();
    auto fsResult = attributeDir->RemoveDirectory(attrPath, indexlib::file_system::RemoveOption::MayNonExist());
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "remove attribute [%s] directory fail, error [%s]", _attributeConfig->GetAttrName().c_str(),
                  fsResult.Status().ToString().c_str());
        return nullptr;
    }

    auto [mkStatus, directory] =
        attributeDir->MakeDirectory(attrPath, indexlib::file_system::DirectoryOption()).StatusWith();
    if (!mkStatus.IsOK()) {
        AUTIL_LOG(ERROR, "create attribute [%s] directory fail, error [%s]", _attributeConfig->GetAttrName().c_str(),
                  mkStatus.ToString().c_str());
        return nullptr;
    }

    indexlib::file_system::WriterOption option;
    FileCompressParamHelper::SyncParam(_attributeConfig->GetFileCompressConfigV2(), segmentStatistics, option);
    auto [status, fileWriter] = directory->CreateFileWriter(ATTRIBUTE_DATA_FILE_NAME, option).StatusWith();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create file writer for attribute [%s] fail, error [%s]",
                  _attributeConfig->GetAttrName().c_str(), status.ToString().c_str());
        return nullptr;
    }

    return fileWriter;
}

template <typename T>
Status SingleValueAttributeMerger<T>::MergePatches(const SegmentMergeInfos segmentMergeInfos)
{
    return DoMergePatches<DedupPatchFileMerger>(segmentMergeInfos, _attributeConfig);
}

template <typename T>
void SingleValueAttributeMerger<T>::FlushDataBuffer(OutputData& outputData)
{
    if (!outputData.dataAppender || outputData.dataAppender->GetInBufferCount() == 0) {
        return;
    }

    if (AttributeCompressInfo::NeedCompressData(_attributeConfig)) {
        FlushCompressDataBuffer(outputData);
        return;
    }
    outputData.dataAppender->Flush();
}

template <typename T>
inline void SingleValueAttributeMerger<T>::FlushCompressDataBuffer(const OutputData& outputData)
{
    assert(_compressDumpers.size() == _segOutputMapper.GetOutputs().size());
    auto compressDumper = _compressDumpers[outputData.outputIdx];
    assert(compressDumper);
    outputData.dataAppender->FlushCompressBuffer(compressDumper.get());
}

template <typename T>
inline Status SingleValueAttributeMerger<T>::DumpCompressDataBuffer()
{
    assert(_compressDumpers.size() == _segOutputMapper.GetOutputs().size());
    for (size_t i = 0; i < _compressDumpers.size(); ++i) {
        auto& compressDumper = _compressDumpers[i];
        if (compressDumper) {
            auto st = compressDumper->Dump(_segOutputMapper.GetOutputs()[i].dataAppender->GetDataFileWriter());
            RETURN_IF_STATUS_ERROR(st, "dump compress data failed.");
            compressDumper->Reset();
        }
    }
    return Status::OK();
}

template <typename T>
void SingleValueAttributeMerger<T>::DestroyBuffers()
{
    _segOutputMapper.Clear();
}

template <typename T>
void SingleValueAttributeMerger<T>::CloseFiles()
{
    for (auto& outputData : _segOutputMapper.GetOutputs()) {
        if (outputData.dataAppender) {
            outputData.dataAppender->Close();
        }
    }
}

} // namespace indexlibv2::index
