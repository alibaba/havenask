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
#include "indexlib/table/normal_table/index_task/InvertedIndexMergeOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/InvertedIndexMerger.h"
#include "indexlib/index/inverted_index/config/TruncateOptionConfig.h"
#include "indexlib/index/inverted_index/config/TruncateProfileConfig.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchFileFinder.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"
#include "indexlib/index/inverted_index/truncate/TruncateMetaFileReaderCreator.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/PatchFileFinderUtil.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, InvertedIndexMergeOperation);

InvertedIndexMergeOperation::InvertedIndexMergeOperation(const indexlibv2::framework::IndexOperationDescription& opDesc)
    : indexlibv2::table::IndexMergeOperation(opDesc)
{
}

Status InvertedIndexMergeOperation::Init(const std::shared_ptr<indexlibv2::config::ITabletSchema> schema)
{
    std::string indexType;
    if (!_desc.GetParameter(indexlibv2::table::MERGE_INDEX_TYPE, indexType)) {
        AUTIL_LOG(ERROR, "get index type failed");
        assert(false);
        return Status::Corruption("get index type failed");
    }
    std::string indexName;
    if (!_desc.GetParameter(indexlibv2::table::MERGE_INDEX_NAME, indexName)) {
        AUTIL_LOG(ERROR, "get index type [%s]'s index name failed", indexType.c_str());
        assert(false);
        return Status::Corruption("get index name failed");
    }

    auto [configStatus, indexConfig] = GetIndexConfig(_desc, indexType, indexName, schema);
    RETURN_IF_STATUS_ERROR(configStatus, "Get Index Config fail for [%s]", indexName.c_str());
    auto creator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = creator->Create(indexType);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
        return status;
    }
    _indexMerger = indexFactory->CreateIndexMerger(indexConfig);
    if (_indexMerger == nullptr) {
        AUTIL_LOG(ERROR, "create index merger faild. index type [%s], index name [%s]", indexType.c_str(),
                  indexName.c_str());
        assert(false);
        return Status::Corruption("create index mergeer failed");
    }

    _indexConfig = indexConfig;
    _indexPath = indexFactory->GetIndexPath();
    return Status::OK();
}

Status InvertedIndexMergeOperation::Execute(const indexlibv2::framework::IndexTaskContext& context)
{
    auto [status, segmentMergeInfos] = PrepareSegmentMergeInfos(context, true);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    std::map<std::string, std::any> params;
    status = GetAllParameters(context, segmentMergeInfos, params);
    RETURN_IF_STATUS_ERROR(status, "get all param failed.");

    status = _indexMerger->Init(_indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "init index merger failed. index type [%s], index name [%s]",
                           _indexConfig->GetIndexType().c_str(), _indexConfig->GetIndexName().c_str());
    try {
        auto [status, opLog2PatchOpRootDir] =
            indexlibv2::table::PatchFileFinderUtil::GetOpLog2PatchRootDir(_desc, context);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);

        indexlibv2::index::PatchInfos allPatchInfos;
        index::InvertedIndexPatchFileFinder patchFinder;
        RETURN_STATUS_DIRECTLY_IF_ERROR(indexlibv2::table::PatchFileFinderUtil::FindPatchInfos(
            &patchFinder, context.GetTabletData(), _indexConfig, opLog2PatchOpRootDir, &allPatchInfos));
        auto invertedIndexMerger = std::dynamic_pointer_cast<index::InvertedIndexMerger>(_indexMerger);
        assert(invertedIndexMerger != nullptr);
        invertedIndexMerger->SetPatchInfos(allPatchInfos);

        auto resourceManager = context.GetResourceManager();
        return _indexMerger->Merge(segmentMergeInfos, resourceManager);
    } catch (const autil::legacy::ExceptionBase& e) {
        auto status = Status::InternalError("inverted merge failed, exception [%s]", e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
}

std::string InvertedIndexMergeOperation::GetDebugString() const
{
    auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(_indexConfig);
    if (invertedIndexConfig && invertedIndexConfig->GetTruncateIndexConfigs().size() == 0) {
        return IndexMergeOperation::GetDebugString();
    } else {
        return IndexMergeOperation::GetDebugString() + ".truncate_" +
               std::to_string(invertedIndexConfig->GetTruncateIndexConfigs().size());
    }
}

Status InvertedIndexMergeOperation::GetAllParameters(
    const indexlibv2::framework::IndexTaskContext& context,
    const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
    std::map<std::string, std::any>& params) const
{
    for (const auto& [k, v] : _desc.GetAllParameters()) {
        params[k] = v;
    }

    auto iter = params.find(indexlibv2::table::MERGE_PLAN_INDEX);
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "cannot find merge plan index");
        return Status::InternalError("cannot find merge plan index");
    }
    params[SEGMENT_MERGE_PLAN_INDEX] = std::any_cast<std::string>(iter->second);
    int64_t srcVersionTimestamp = context.GetTabletData()->GetOnDiskVersion().GetTimestamp() / 1000000;
    params[SOURCE_VERSION_TIMESTAMP] = srcVersionTimestamp;

    auto resourceManager = context.GetResourceManager();
    std::string docMapperName;
    if (!_desc.GetParameter(indexlibv2::index::DocMapper::GetDocMapperType(), docMapperName)) {
        AUTIL_LOG(ERROR, "could't find doc mapper, type [%s]",
                  indexlibv2::index::DocMapper::GetDocMapperType().c_str());
        return Status::Corruption();
    }
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto status =
        resourceManager->LoadResource(docMapperName, indexlibv2::index::DocMapper::GetDocMapperType(), docMapper);
    RETURN_IF_STATUS_ERROR(status, "load doc mapper failed.");

    auto schema = context.GetTabletSchema();
    auto truncateAttributeReaderCreator =
        std::make_shared<indexlib::index::TruncateAttributeReaderCreator>(schema, segmentMergeInfos, docMapper);
    assert(truncateAttributeReaderCreator != nullptr);
    params[TRUNCATE_ATTRIBUTE_READER_CREATOR] = truncateAttributeReaderCreator;

    auto [st, truncateProfileConfigs] =
        schema->GetRuntimeSettings().GetValue<std::vector<indexlibv2::config::TruncateProfileConfig>>(
            "truncate_profiles");
    if (!st.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "get truncate profile settings failed.");
        return st;
    }
    auto mergeConfig = context.GetMergeConfig();
    const auto* truncateStrategy = std::any_cast<std::vector<indexlibv2::config::TruncateStrategy>>(
        mergeConfig.GetHookOption(index::TRUNCATE_STRATEGY));
    if (!truncateStrategy) {
        status = Status::Corruption("get truncate strategy from merge config failed");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto truncateOptionConfig = std::make_shared<indexlibv2::config::TruncateOptionConfig>(*truncateStrategy);
    truncateOptionConfig->Init({_indexConfig}, truncateProfileConfigs);
    params[TRUNCATE_OPTION_CONFIG] = truncateOptionConfig;

    auto rootDir = context.GetIndexRoot()->GetIDirectory();
    std::shared_ptr<indexlib::file_system::IDirectory> truncateMetaDir;
    RETURN_IF_STATUS_ERROR(PrepareTruncateMetaDir(rootDir, &truncateMetaDir), "prepare truncate meta dir failed.");
    auto truncateMetaFileReaderCreator =
        std::make_shared<indexlib::index::TruncateMetaFileReaderCreator>(truncateMetaDir);
    params[TRUNCATE_META_FILE_READER_CREATOR] = truncateMetaFileReaderCreator;
    return Status::OK();
}

Status
InvertedIndexMergeOperation::PrepareTruncateMetaDir(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                                    std::shared_ptr<indexlib::file_system::IDirectory>* result)
{
    auto [ec, truncateMetaDir] = rootDir->GetDirectory(TRUNCATE_META_DIR_NAME);
    if (ec != indexlib::file_system::FSEC_OK) {
        if (ec == indexlib::file_system::FSEC_NOENT) {
            auto [st, dir] =
                rootDir->MakeDirectory(TRUNCATE_META_DIR_NAME, indexlib::file_system::DirectoryOption()).StatusWith();
            *result = dir;
            return st;
        }
        return toStatus(ec);
    }
    auto status = rootDir->GetFileSystem()
                      ->MountDir(/*physicalRoot=*/rootDir->GetPhysicalPath(""), /*physicalPath=*/TRUNCATE_META_DIR_NAME,
                                 /*logicalPath=*/TRUNCATE_META_DIR_NAME,
                                 /*MountOption=*/indexlib::file_system::FSMT_READ_WRITE,
                                 /*enableLazyMount=*/false)
                      .Status();
    RETURN_IF_STATUS_ERROR(status, "mount truncate meta dir failed.");
    *result = truncateMetaDir;
    return Status::OK();
}

} // namespace indexlib::table
