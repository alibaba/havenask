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

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDataInfo.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/merger/AttributeMerger.h"
#include "indexlib/index/attribute/merger/AttributeMergerCreator.h"
#include "indexlib/index/attribute/merger/DocumentMergeInfoHeap.h"
#include "indexlib/index/attribute/merger/SegmentOutputMapper.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/attribute/patch/DedupPatchFileMerger.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributeMerger : public AttributeMerger
{
public:
    MultiValueAttributeMerger();
    virtual ~MultiValueAttributeMerger();

public:
    using DiskIndexerWithCtx =
        std::pair<std::shared_ptr<AttributeDiskIndexer>, std::shared_ptr<AttributeDiskIndexer::ReadContextBase>>;

protected:
    struct OutputData {
        size_t outputIdx = 0;
        std::shared_ptr<indexlib::file_system::FileWriter> fileWriter;
        std::shared_ptr<VarLenDataWriter> dataWriter;
    };

public:
    DECLARE_ATTRIBUTE_MERGER_IDENTIFIER(multi);
    Status DoMerge(const SegmentMergeInfos& segMergeInfos, std::shared_ptr<DocMapper>& docMapper) override;

protected:
    virtual Status PrepareOutputDatas(const std::shared_ptr<DocMapper>& docMapper,
                                      const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegments);
    virtual void DestroyOutputDatas();
    virtual Status CloseFiles();
    virtual Status ReadData(docid_t docId, const DiskIndexerWithCtx& diskIndexerWithCtx, uint8_t* dataBuf,
                            uint32_t dataBufLen, uint32_t& dataLen);
    virtual Status MergeData(const std::shared_ptr<DocMapper>& docMapper,
                             const IIndexMerger::SegmentMergeInfos& segMergeInfos);
    virtual std::pair<Status, std::shared_ptr<IIndexer>>
    GetIndexerFromSegment(const std::shared_ptr<framework::Segment>& segment,
                          const std::shared_ptr<AttributeConfig>& attrConfig);

    void ReserveMemBuffers(const std::vector<DiskIndexerWithCtx>& diskIndexers);

    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir) override;

private:
    void ReleaseMemBuffers();
    Status DumpDataInfoFile();
    Status CreateDiskIndexers(const SegmentMergeInfos& segMergeInfos);
    void EnsureReadCtx();
    Status MergePatches(const SegmentMergeInfos segmentMergeInfos);

protected:
    std::vector<DiskIndexerWithCtx> _diskIndexers;
    SegmentOutputMapper<OutputData> _segOutputMapper;
    indexlib::util::MemBuffer _dataBuf;

private:
    indexlib::util::SimplePool _pool;
    autil::mem_pool::UnsafePool _readPool;
    size_t _switchLimit;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributeMerger, T);

template <typename T>
MultiValueAttributeMerger<T>::MultiValueAttributeMerger() : AttributeMerger()
{
    _switchLimit = autil::EnvUtil::getEnv("MERGE_SWITCH_MEMORY_LIMIT", 100 * 1024 * 1024);
    // default 100M
}

template <typename T>
MultiValueAttributeMerger<T>::~MultiValueAttributeMerger()
{
    _diskIndexers.clear();
    // must release dumper before pool released: mSegOutputMapper shuold declared after mPool
}

template <typename T>
Status MultiValueAttributeMerger<T>::DoMerge(const SegmentMergeInfos& segMergeInfos,
                                             std::shared_ptr<DocMapper>& docMapper)
{
    RETURN_IF_STATUS_ERROR(CreateDiskIndexers(segMergeInfos), "create disk indexer failed");
    auto status = PrepareOutputDatas(docMapper, segMergeInfos.targetSegments);
    RETURN_IF_STATUS_ERROR(status, "prepare output data failed");

    status = MergeData(docMapper, segMergeInfos);
    RETURN_IF_STATUS_ERROR(status, "do merge operation failed in multi value attribute merger");

    _diskIndexers.clear();
    status = CloseFiles();
    RETURN_IF_STATUS_ERROR(status, "close file failed in multi value attribute merger.");
    DestroyOutputDatas();
    ReleaseMemBuffers();

    status = MergePatches(segMergeInfos);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "merge patch failed.");
        return status;
    }
    return Status::OK();
}

template <typename T>
inline std::pair<Status, std::shared_ptr<IIndexer>>
MultiValueAttributeMerger<T>::GetIndexerFromSegment(const std::shared_ptr<framework::Segment>& segment,
                                                    const std::shared_ptr<AttributeConfig>& attrConfig)
{
    return segment->GetIndexer(attrConfig->GetIndexType(), attrConfig->GetIndexName());
}

template <typename T>
inline Status MultiValueAttributeMerger<T>::CreateDiskIndexers(const SegmentMergeInfos& segMergeInfos)
{
    for (const auto& srcSegment : segMergeInfos.srcSegments) {
        auto& segment = srcSegment.segment;
        if (segment->GetSegmentInfo()->docCount == 0) {
            _diskIndexers.emplace_back(std::make_pair(nullptr, nullptr));
            continue;
        }

        std::shared_ptr<AttributeDiskIndexer> diskIndexer;
        auto indexerPair = GetIndexerFromSegment(segment, _attributeConfig);
        if (!indexerPair.first.IsOK()) {
            auto segmentSchema = segment->GetSegmentSchema();
            if (indexerPair.first.IsNotFound() && segmentSchema &&
                segmentSchema->GetIndexConfig(ATTRIBUTE_INDEX_TYPE_STR, _attributeConfig->GetAttrName()) == nullptr) {
                auto attributeConfigWithoutSlice = _attributeConfig->CreateSliceAttributeConfigs(1)[0];
                auto [status, defaultIndexer] =
                    AttributeDefaultDiskIndexerFactory::CreateDefaultDiskIndexer(segment, attributeConfigWithoutSlice);
                RETURN_IF_STATUS_ERROR(status, "create default indexer for merge failed");
                diskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(defaultIndexer);
                assert(diskIndexer);
            } else {
                AUTIL_LOG(ERROR, "no attribute indexer for [%s] in segment [%d]",
                          _attributeConfig->GetIndexName().c_str(), segment->GetSegmentId());
                return Status::InternalError("no attribute indexer for [%s] in segment [%d]",
                                             _attributeConfig->GetIndexName().c_str(), segment->GetSegmentId());
            }
        } else {
            auto indexer = indexerPair.second;
            diskIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(indexer);
            assert(diskIndexer != nullptr);
        }
        _diskIndexers.emplace_back(std::make_pair(diskIndexer, diskIndexer->CreateReadContextPtr(&_readPool)));
    }
    return Status::OK();
}

template <typename T>
Status MultiValueAttributeMerger<T>::PrepareOutputDatas(
    const std::shared_ptr<DocMapper>& docMapper,
    const std::vector<std::shared_ptr<framework::SegmentMeta>>& targetSegmentMetas)
{
    auto createFunc = [this](const framework::SegmentMeta& segmentMeta, size_t outputIdx,
                             OutputData& output) -> Status {
        output.outputIdx = outputIdx;

        assert(segmentMeta.segmentDir != nullptr);
        auto segIDir = segmentMeta.segmentDir->GetIDirectory();
        auto [status, attributeDir] = GetOutputDirectory(segIDir);

        RETURN_IF_STATUS_ERROR(status, "get output diretory [%s] fail for index [%s]. error: [%s]",
                               segIDir->DebugString().c_str(), _attributeConfig->GetAttrName().c_str(),
                               status.ToString().c_str());

        std::string attrPath = _attributeConfig->GetAttrName() + "/" + _attributeConfig->GetSliceDir();
        auto [dirStatus, fieldDir] =
            attributeDir->MakeDirectory(attrPath, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN_IF_STATUS_ERROR(dirStatus, "make diretory fail. file: [%s], error: [%s]", attrPath.c_str(),
                               dirStatus.ToString().c_str());

        auto [writerStatus, fileWriter] =
            fieldDir->CreateFileWriter(ATTRIBUTE_DATA_INFO_FILE_NAME, indexlib::file_system::WriterOption())
                .StatusWith();
        RETURN_IF_STATUS_ERROR(writerStatus, "create file writer fail. file: [%s], error: [%s]",
                               index::ATTRIBUTE_INDEX_TYPE_STR.c_str(), status.ToString().c_str());
        output.fileWriter = fileWriter;
        output.dataWriter.reset(new VarLenDataWriter(&_pool));
        auto param = VarLenDataParamHelper::MakeParamForAttribute(_attributeConfig);
        assert(segmentMeta.segmentInfo);
        auto [statisticStatus, segmentStatistics] = segmentMeta.segmentInfo->GetSegmentStatistics();
        auto segmentStatisticsPtr = std::make_shared<framework::SegmentStatistics>(segmentStatistics);
        RETURN_IF_STATUS_ERROR(statisticStatus, "jsonize segment statistics failed");
        FileCompressParamHelper::SyncParam(_attributeConfig->GetFileCompressConfigV2(), segmentStatisticsPtr, param);
        status =
            output.dataWriter->Init(fieldDir, ATTRIBUTE_OFFSET_FILE_NAME, ATTRIBUTE_DATA_FILE_NAME, nullptr, param);
        RETURN_IF_STATUS_ERROR(status, "init VarLenDataWriter failed. ");

        AUTIL_LOG(INFO, "create output data for dir [%s]",
                  segmentMeta.segmentDir->DebugString(_attributeConfig->GetAttrName()).c_str());
        return Status::OK();
    };

    auto status = _segOutputMapper.Init(docMapper, targetSegmentMetas, createFunc);
    return status;
}

template <typename T>
inline void MultiValueAttributeMerger<T>::DestroyOutputDatas()
{
    _segOutputMapper.Clear();
    _dataBuf.Release();
}

template <typename T>
inline Status MultiValueAttributeMerger<T>::CloseFiles()
{
    auto status = DumpDataInfoFile();
    RETURN_IF_STATUS_ERROR(status, "dump data file info failed.");

    for (auto& output : _segOutputMapper.GetOutputs()) {
        status = output.dataWriter->Close();
        RETURN_IF_STATUS_ERROR(status, "close VarLenDataWriter failed.");
        status = output.fileWriter->Close().Status();
        RETURN_IF_STATUS_ERROR(status, "close FileWriter failed.");
    }
    return Status::OK();
}

template <typename T>
inline void MultiValueAttributeMerger<T>::ReleaseMemBuffers()
{
    _dataBuf.Release();
}

template <typename T>
inline Status MultiValueAttributeMerger<T>::MergeData(const std::shared_ptr<DocMapper>& docMapper,
                                                      const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    SliceInfo sliceInfo(_attributeConfig->GetSliceCount(), _attributeConfig->GetSliceIdx());
    docid_t beginDocId = 0;
    docid_t endDocId = std::numeric_limits<docid_t>::max();
    if (_attributeConfig->GetSliceCount() > 1 && _attributeConfig->GetSliceIdx() >= 0) {
        if (segMergeInfos.targetSegments.size() != 1) {
            AUTIL_LOG(ERROR, "not support attribute slice with merge to multi segment");
            return Status::Corruption();
        }
        int64_t docCount = docMapper->GetTargetSegmentDocCount(segMergeInfos.targetSegments[0]->segmentId);
        sliceInfo.GetDocRange(docCount, beginDocId, endDocId);
    }

    DocumentMergeInfoHeap heap;
    heap.Init(segMergeInfos, docMapper);

    ReserveMemBuffers(_diskIndexers);
    DocumentMergeInfo info;

    while (heap.GetNext(info)) {
        auto output = _segOutputMapper.GetOutput(info.oldDocId);
        if (!output) {
            continue;
        }
        if (info.newDocId < beginDocId || info.newDocId > endDocId) {
            // not in attribute slice ignore
            continue;
        }
        int32_t segIdx = info.segmentIndex;
        docid_t curBaseDocId = segMergeInfos.srcSegments[segIdx].baseDocid;
        docid_t currentLocalId = info.oldDocId - curBaseDocId;
        uint32_t dataLen = 0;
        auto status = ReadData(currentLocalId, _diskIndexers[segIdx], (uint8_t*)_dataBuf.GetBuffer(),
                               _dataBuf.GetBufferSize(), dataLen);
        RETURN_IF_STATUS_ERROR(status, "read multi value attribute data failed");
        autil::StringView dataValue((const char*)_dataBuf.GetBuffer(), dataLen);
        auto st = output->dataWriter->AppendValue(dataValue);
        RETURN_IF_STATUS_ERROR(st, "append value failed in VarLenDataWriter. ");
    }
    return Status::OK();
}

template <typename T>
Status MultiValueAttributeMerger<T>::ReadData(docid_t docId, const DiskIndexerWithCtx& diskIndexerWithCtx,
                                              uint8_t* dataBuf, uint32_t dataBufLen, uint32_t& dataLen)
{
    auto& [diskIndexer, ctx] = diskIndexerWithCtx;
    bool isNull = false;
    dataLen = 0;
    EnsureReadCtx();
    if (!diskIndexer->Read(docId, ctx, dataBuf, dataBufLen, dataLen, isNull)) {
        AUTIL_LOG(ERROR, "read attribute data for doc [%d] failed.", docId);
        return Status::InternalError("read attribute data failed for doc ", docId);
    }
    return Status::OK();
}

template <typename T>
inline void MultiValueAttributeMerger<T>::EnsureReadCtx()
{
    if (_readPool.getUsedBytes() >= _switchLimit) {
        AUTIL_LOG(WARN, "switch ctx, may be need expend chunk memory size (if this log often happened, set env "
                        "MERGE_SWITCH_MEMORY_LIMIT)");
        for (auto& [diskIndexer, ctx] : _diskIndexers) {
            ctx.reset();
        }
        _readPool.release();
    }
    for (auto& [diskIndexer, ctx] : _diskIndexers) {
        if (diskIndexer) {
            if (ctx) {
                break;
            }
            ctx = diskIndexer->CreateReadContextPtr(&_readPool);
        }
    }
}

template <typename T>
std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
MultiValueAttributeMerger<T>::GetOutputDirectory(const std::shared_ptr<indexlib::file_system::IDirectory>& segDir)
{
    return segDir->MakeDirectory(_attributeConfig->GetIndexCommonPath(), indexlib::file_system::DirectoryOption())
        .StatusWith();
}

template <typename T>
inline void MultiValueAttributeMerger<T>::ReserveMemBuffers(const std::vector<DiskIndexerWithCtx>& diskIndexers)
{
    uint32_t maxItemLen = 0;
    // TODO(yanke & liuyuping): here if the segment doc is null, add patch file's mem buffer?
    for (size_t i = 0; i < diskIndexers.size(); i++) {
        auto diskIndexer = diskIndexers[i].first;
        if (diskIndexer) {
            maxItemLen = std::max(diskIndexer->GetAttributeDataInfo().maxItemLen, maxItemLen);
        }
    }
    _dataBuf.Reserve(maxItemLen);
}

template <typename T>
Status MultiValueAttributeMerger<T>::MergePatches(const SegmentMergeInfos segmentMergeInfos)
{
    return DoMergePatches<DedupPatchFileMerger>(segmentMergeInfos, _attributeConfig);
}

template <typename T>
Status MultiValueAttributeMerger<T>::DumpDataInfoFile()
{
    for (auto& output : _segOutputMapper.GetOutputs()) {
        AttributeDataInfo dataInfo(output.dataWriter->GetDataItemCount(), output.dataWriter->GetMaxItemLength());
        std::string content = dataInfo.ToString();

        auto status = output.fileWriter->Write(content.c_str(), content.length()).Status();
        RETURN_IF_STATUS_ERROR(status, "write file info failed.");
    }
    return Status::OK();
}

} // namespace indexlibv2::index
