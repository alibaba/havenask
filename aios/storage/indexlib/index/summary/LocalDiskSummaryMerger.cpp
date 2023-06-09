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
#include "indexlib/index/summary/LocalDiskSummaryMerger.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataMerger.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/LocalDiskSummaryDiskIndexer.h"
#include "indexlib/index/summary/SummaryDiskIndexer.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, LocalDiskSummaryMerger);

LocalDiskSummaryMerger::LocalDiskSummaryMerger(const std::shared_ptr<config::SummaryIndexConfig>& summaryIndexConfig,
                                               summarygroupid_t groupId)
    : _summaryIndexConfig(summaryIndexConfig)
    , _groupId(groupId)
{
    assert(groupId >= 0 && groupId < _summaryIndexConfig->GetSummaryGroupConfigCount());
    _summaryGroupConfig = _summaryIndexConfig->GetSummaryGroupConfig(_groupId);
}

Status LocalDiskSummaryMerger::Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                     const std::shared_ptr<DocMapper>& docMapper)
{
    auto groupId = _summaryGroupConfig->GetGroupId();
    if (!_summaryGroupConfig->NeedStoreSummary()) {
        AUTIL_LOG(
            DEBUG,
            "do nothing for current summary group [%d]'s merger because of summary configuration [!NeedStoreSummary]",
            groupId);
        return Status::OK();
    }
    auto [status, merger] = PrepareVarLenDataMerger(segMergeInfos, docMapper);
    RETURN_IF_STATUS_ERROR(status, "create VarLenDataMerger file for summary group [%d]", groupId);
    return merger->Merge();
}

std::pair<Status, std::shared_ptr<VarLenDataMerger>>
LocalDiskSummaryMerger::PrepareVarLenDataMerger(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                                const std::shared_ptr<DocMapper>& docMapper)
{
    auto param = CreateVarLenDataParam();
    std::vector<VarLenDataMerger::InputData> inputDatas;
    const auto& srcSegments = segMergeInfos.srcSegments;
    for (size_t i = 0; i < srcSegments.size(); i++) {
        const auto& srcSegmentInfo = srcSegments[i].segment->GetSegmentInfo();
        VarLenDataMerger::InputData input;
        if (srcSegmentInfo->docCount == 0) {
            input.dataReader = nullptr;
            inputDatas.push_back(input);
            continue;
        }

        auto [status, dataReader] = CreateSegmentReader(srcSegments[i]);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "collect segment writer fail for segmenty [%d]",
                                srcSegments[i].segment->GetSegmentId());
        assert(dataReader != nullptr);

        input.dataReader = dataReader;
        inputDatas.push_back(input);
    }

    std::vector<VarLenDataMerger::OutputData> outputDatas;
    const auto& targetSegments = segMergeInfos.targetSegments;
    for (size_t i = 0; i < targetSegments.size(); ++i) {
        VarLenDataMerger::OutputData output;
        auto [status, dataWriter] = CreateSegmentWriter(param, targetSegments[i]);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "create segment writer fail for segment [%d]",
                                targetSegments[i]->segmentId);
        output.dataWriter = dataWriter;
        output.targetSegmentId = targetSegments[i]->segmentId;
        outputDatas.push_back(output);
    }
    auto merger = std::make_shared<VarLenDataMerger>(param);
    merger->Init(segMergeInfos, docMapper, inputDatas, outputDatas);
    return std::make_pair(Status::OK(), merger);
}

std::pair<Status, std::shared_ptr<VarLenDataReader>>
LocalDiskSummaryMerger::CreateSegmentReader(const IIndexMerger::SourceSegment& srcSegment)
{
    auto& segment = srcSegment.segment;
    if (segment->GetSegmentInfo()->docCount == 0) {
        return std::make_pair(Status::OK(), nullptr);
    }
    auto segmentDirectory = segment->GetSegmentDirectory()->GetIDirectory();
    auto [status, summaryDirectory] =
        segmentDirectory->GetDirectory(_summaryIndexConfig->GetIndexPath()[0]).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get summary directory for segment [%d] failed", segment->GetSegmentId());
    return LocalDiskSummaryDiskIndexer::GetSummaryVarLenDataReader(segment->GetSegmentInfo()->docCount,
                                                                   _summaryGroupConfig, summaryDirectory, false);
}

std::pair<Status, std::shared_ptr<VarLenDataWriter>>
LocalDiskSummaryMerger::CreateSegmentWriter(const VarLenDataParam& param,
                                            const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta)
{
    // directory must be clean (empty)
    auto [status, directory] = CreateOutputDirectory(outputSegmentMeta);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create output directory fail in merge operation");
    fslib::FileList fileList;

    assert(_summaryGroupConfig);
    auto fileCompressConfig = _summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfigV2();

    VarLenDataParam outputParam = param;
    auto [status2, segmentStats] = outputSegmentMeta->segmentInfo->GetSegmentStatistics();
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "get segment stats failed");
    FileCompressParamHelper::SyncParam(fileCompressConfig, std::make_shared<framework::SegmentStatistics>(segmentStats),
                                       outputParam);

    auto dataWriter = std::make_shared<VarLenDataWriter>(&_pool);
    status = dataWriter->Init(directory, SUMMARY_OFFSET_FILE_NAME, SUMMARY_DATA_FILE_NAME, nullptr, outputParam);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "init VarLenDataWriter fail");
    return std::make_pair(Status::OK(), dataWriter);
}

VarLenDataParam LocalDiskSummaryMerger::CreateVarLenDataParam() const
{
    return VarLenDataParamHelper::MakeParamForSummary(_summaryGroupConfig);
}

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
LocalDiskSummaryMerger::CreateOutputDirectory(const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta) const
{
    assert(outputSegmentMeta->segmentDir != nullptr);
    auto segIDir = outputSegmentMeta->segmentDir->GetIDirectory();
    auto [status, summaryDir] =
        segIDir->MakeDirectory(index::SUMMARY_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "make diretory fail. dir: [%s], error: [%s]",
                            index::SUMMARY_INDEX_PATH.c_str(), status.ToString().c_str());
    if (!_summaryGroupConfig->IsDefaultGroup()) {
        const std::string& groupName = _summaryGroupConfig->GetGroupName();
        std::tie(status, summaryDir) =
            summaryDir->MakeDirectory(groupName, indexlib::file_system::DirectoryOption()).StatusWith();
        RETURN2_IF_STATUS_ERROR(status, nullptr, "make group diretory fail. dir: [%s], group dir [%s], error: [%s]",
                                index::SUMMARY_INDEX_PATH.c_str(), groupName.c_str(), status.ToString().c_str());
    }
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    status = summaryDir->RemoveFile(SUMMARY_OFFSET_FILE_NAME, /*mayNonExist*/ removeOption).Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    status = summaryDir->RemoveFile(SUMMARY_DATA_FILE_NAME, /*mayNonExist*/ removeOption).Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    status = summaryDir
                 ->RemoveFile(std::string(SUMMARY_OFFSET_FILE_NAME) +
                                  std::string(indexlib::file_system::COMPRESS_FILE_INFO_SUFFIX),
                              /*mayNonExist*/ removeOption)
                 .Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    status = summaryDir
                 ->RemoveFile(std::string(SUMMARY_OFFSET_FILE_NAME) +
                                  std::string(indexlib::file_system::COMPRESS_FILE_META_SUFFIX),
                              /*mayNonExist*/ removeOption)
                 .Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    status = summaryDir
                 ->RemoveFile(std::string(SUMMARY_DATA_FILE_NAME) +
                                  std::string(indexlib::file_system::COMPRESS_FILE_INFO_SUFFIX),
                              /*mayNonExist*/ removeOption)
                 .Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    status = summaryDir
                 ->RemoveFile(std::string(SUMMARY_DATA_FILE_NAME) +
                                  std::string(indexlib::file_system::COMPRESS_FILE_META_SUFFIX),
                              /*mayNonExist*/ removeOption)
                 .Status();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "remove summary offset file fail. error: [%s]", status.ToString().c_str());

    return std::make_pair(Status::OK(), summaryDir);
}

} // namespace indexlibv2::index
