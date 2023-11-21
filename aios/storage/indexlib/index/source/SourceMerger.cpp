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
#include "indexlib/index/source/SourceMerger.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataMerger.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataReader.h"
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlibv2.index, SourceMerger);

Status SourceMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                          const std::map<std::string, std::any>& params)
{
    _sourceConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(indexConfig);
    if (!_sourceConfig) {
        RETURN_STATUS_ERROR(InvalidArgs, "source config is nullptr");
    }
    std::optional<size_t> mergeIdOpt = _sourceConfig->GetMergeId();
    if (!mergeIdOpt || mergeIdOpt.value() > _sourceConfig->GetSourceGroupCount()) {
        RETURN_STATUS_ERROR(InvalidArgs, "merge id is invalid");
    }
    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        RETURN_STATUS_ERROR(Corruption, "could't find doc mapper, type [%s]", DocMapper::GetDocMapperType().c_str());
    }
    _docMapperName = std::any_cast<std::string>(iter->second);
    AUTIL_LOG(INFO, "init source merge finish, merge id [%lu]", mergeIdOpt.value());
    return Status::OK();
}
Status SourceMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                           const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    std::shared_ptr<DocMapper> docMapper;
    RETURN_IF_STATUS_ERROR(taskResourceManager->LoadResource<DocMapper>(/*name=*/_docMapperName,
                                                                        /*resourceType=*/DocMapper::GetDocMapperType(),
                                                                        docMapper),
                           "load doc mappaer failed");

    auto [status, merger] = PrepareVarLenDataMerger(segMergeInfos, docMapper);
    RETURN_IF_STATUS_ERROR(status, "create VarLenDataMerger file for source mergeId [%lu]",
                           _sourceConfig->GetMergeId().value());
    return merger->Merge();
}

std::pair<Status, std::shared_ptr<VarLenDataMerger>>
SourceMerger::PrepareVarLenDataMerger(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                      const std::shared_ptr<DocMapper>& docMapper)
{
    VarLenDataParam param =
        IsMetaMerge() ? VarLenDataParamHelper::MakeParamForSourceMeta()
                      : VarLenDataParamHelper::MakeParamForSourceData(_sourceConfig->GetGroupConfig(GetDataGroupId()));

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

        auto [status, dataReader] = CreateSegmentReader(param, srcSegments[i]);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "collect segment writer fail for segment [%d]",
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

bool SourceMerger::IsMetaMerge() const
{
    size_t mergeId = _sourceConfig->GetMergeId().value();
    return mergeId == _sourceConfig->GetSourceGroupCount();
}

index::sourcegroupid_t SourceMerger::GetDataGroupId() const
{
    assert(!IsMetaMerge());
    return _sourceConfig->GetMergeId().value();
}

std::string SourceMerger::GetOutputPathFromSegment() const
{
    return IsMetaMerge() ? SOURCE_META_DIR : GetDataDir(GetDataGroupId());
}

std::pair<Status, std::shared_ptr<VarLenDataReader>>
SourceMerger::CreateSegmentReader(const VarLenDataParam& baseParam, const IIndexMerger::SourceSegment& srcSegment)
{
    auto param = baseParam;
    auto& segment = srcSegment.segment;
    if (segment->GetSegmentInfo()->docCount == 0) {
        return std::make_pair(Status::OK(), nullptr);
    }
    auto segmentDirectory = segment->GetSegmentDirectory()->GetIDirectory();
    auto [status, sourceDirectory] = segmentDirectory->GetDirectory(SOURCE_INDEX_PATH).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get source directory for segment [%d] failed", segment->GetSegmentId());
    param.dataCompressorName = "uncertain";

    auto dataReader = std::make_shared<VarLenDataReader>(param, /*is online = */ false);
    assert(sourceDirectory != nullptr);
    auto dirPath = GetOutputPathFromSegment();

    auto [st, readDir] = sourceDirectory->GetDirectory(dirPath).StatusWith();
    RETURN2_IF_STATUS_ERROR(st, nullptr, "get source reader directory failed");
    RETURN2_IF_STATUS_ERROR(
        dataReader->Init(segment->GetSegmentInfo()->docCount, readDir, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME),
        nullptr, "init data reader fail for source");

    return {Status::OK(), dataReader};
}

std::pair<Status, std::shared_ptr<VarLenDataWriter>>
SourceMerger::CreateSegmentWriter(const VarLenDataParam& param,
                                  const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta)
{
    auto segIDir = outputSegmentMeta->segmentDir->GetIDirectory();
    auto [st, sourceBaseDir] =
        segIDir->MakeDirectory(SOURCE_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(st, nullptr, "make source directory failed");

    auto [status, directory] =
        sourceBaseDir->MakeDirectory(GetOutputPathFromSegment(), indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create output directory fail in merge operation");

    assert(_sourceConfig);
    std::shared_ptr<config::FileCompressConfigV2> fileCompressConfig =
        IsMetaMerge() ? nullptr
                      : _sourceConfig->GetGroupConfig(GetDataGroupId())->GetParameter().GetFileCompressConfigV2();
    VarLenDataParam outputParam = param;
    auto [status2, segmentStats] = outputSegmentMeta->segmentInfo->GetSegmentStatistics();
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "get segment stats failed");
    FileCompressParamHelper::SyncParam(fileCompressConfig, std::make_shared<framework::SegmentStatistics>(segmentStats),
                                       outputParam);

    auto dataWriter = std::make_shared<VarLenDataWriter>(&_pool);
    status = dataWriter->Init(directory, SOURCE_OFFSET_FILE_NAME, SOURCE_DATA_FILE_NAME, nullptr, outputParam);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "init VarLenDataWriter fail");
    return std::make_pair(Status::OK(), dataWriter);
}

} // namespace indexlibv2::index
