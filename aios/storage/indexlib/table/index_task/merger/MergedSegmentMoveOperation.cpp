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
#include "indexlib/table/index_task/merger/MergedSegmentMoveOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/MergePackageUtil.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MergedSegmentMoveOperation);

MergedSegmentMoveOperation::MergedSegmentMoveOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

MergedSegmentMoveOperation::~MergedSegmentMoveOperation() {}

Status MergedSegmentMoveOperation::GetFenceSegmentDirectory(
    const framework::IndexTaskContext& context, framework::IndexOperationId opId, const std::string& segDirName,
    std::shared_ptr<indexlib::file_system::IDirectory>* segmentDirectory)
{
    auto fenceDirectory = context.GetDependOperationFenceRoot(opId);
    if (fenceDirectory == nullptr) {
        return Status::IOError("get fence dir of op [%ld] failed", opId);
    }
    auto [status, tmpSegmentDirectory] = fenceDirectory->GetDirectory(segDirName).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "get segment dir [%s] failed in dir[%s] of op [%ld]", segDirName.c_str(),
                           fenceDirectory->GetLogicalPath().c_str(), opId);
    if (tmpSegmentDirectory == nullptr) {
        return Status::Corruption("get segment dir [%s] failed in dir[%s] of op [%ld]", segDirName.c_str(),
                                  fenceDirectory->GetLogicalPath().c_str(), opId);
    }
    *segmentDirectory = tmpSegmentDirectory;
    return Status::OK();
}

Status MergedSegmentMoveOperation::CollectInfoInSegment(
    const framework::IndexTaskContext& context, framework::IndexOperationId opId, const std::string& segDirName,
    const std::string& indexPath, const indexlib::file_system::PackageFileTagConfigList& packageFileTagConfigList,
    indexlib::file_system::MergePackageMeta* mergePackageMeta)
{
    auto fenceDirectory = context.GetDependOperationFenceRoot(opId);
    if (fenceDirectory == nullptr) {
        return Status::IOError("get fence dir of op [%ld] failed", opId);
    }
    std::string listDirPath = PathUtil::JoinPath(segDirName, indexPath);
    auto [status, isExist] = fenceDirectory->IsExist(listDirPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "check dir[%s] exist failed in dir[%s]", listDirPath.c_str(),
                           fenceDirectory->GetPhysicalPath("").c_str());
    if (!isExist) {
        return Status::OK();
    }
    mergePackageMeta->dirSet.emplace(indexPath);
    std::vector<std::string> fileList;
    RETURN_IF_STATUS_ERROR(
        fenceDirectory->ListDir(listDirPath, indexlib::file_system::ListOption::Recursive(), fileList).Status(),
        "list dir[%s] failed in dir[%s]", listDirPath.c_str(), fenceDirectory->GetPhysicalPath("").c_str());
    for (const auto& fileName : fileList) {
        auto [st, isDir] = fenceDirectory->IsDir(PathUtil::JoinPath(listDirPath, fileName)).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "check dir[%s] failed in dir[%s]", fileName.c_str(),
                               fenceDirectory->GetPhysicalPath("").c_str());
        std::string filePath = PathUtil::JoinPath(listDirPath, fileName);
        if (!isDir) {
            ASSIGN_OR_RETURN(size_t size, fenceDirectory->GetFileLength(filePath).StatusWith());
            std::string tag = packageFileTagConfigList.Match(filePath, /*defaultTag=*/"");
            std::map<std::string, size_t>& taggedFileSizeMap = (mergePackageMeta->packageTag2File2SizeMap)[tag];
            taggedFileSizeMap.emplace(fenceDirectory->GetPhysicalPath(filePath), size);
        }
        mergePackageMeta->dirSet.emplace(
            indexlib::util::PathUtil::GetDirectoryPath(PathUtil::JoinPath(indexPath, fileName)));
    }
    return Status::OK();
}

Status MergedSegmentMoveOperation::CollectFileSizesAndDir(
    const framework::IndexTaskContext& context, const std::string& segDirName,
    const indexlib::file_system::PackageFileTagConfigList& packageFileTagConfigList,
    indexlib::file_system::MergePackageMeta* mergePackageMeta)
{
    std::string opToIndexDirStr;
    // legacy code, for old merge, TODO: by yijie.zhang delete
    if (_desc.GetParameter(PARAM_OP_TO_INDEX_DIR, opToIndexDirStr)) {
        std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexDir;
        autil::legacy::FromJsonString(opToIndexDir, opToIndexDirStr);
        for (const auto& opDirPair : opToIndexDir) {
            RETURN_STATUS_DIRECTLY_IF_ERROR(CollectInfoInSegment(context, /*opId=*/opDirPair.first, segDirName,
                                                                 /*indexPath=*/opDirPair.second.second,
                                                                 packageFileTagConfigList, mergePackageMeta));
        }
        return Status::OK();
    }

    if (!_desc.GetParameter(PARAM_OP_TO_INDEX, opToIndexDirStr)) {
        return Status::Corruption("get op to index dir from desc failed");
    }
    std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexs;
    autil::legacy::FromJsonString(opToIndexs, opToIndexDirStr);
    auto schema = context.GetTabletSchema();
    for (const auto& [opId, kvItem] : opToIndexs) {
        const auto& [indexType, indexName] = kvItem;
        auto indexConfig = schema->GetIndexConfig(indexType, indexName);
        for (const auto& innerIndexConfig : GetAllIndexConfigs(schema, indexType, indexName)) {
            std::vector<std::string> indexPaths = innerIndexConfig->GetIndexPath();
            for (const auto& indexPath : indexPaths) {
                RETURN_STATUS_DIRECTLY_IF_ERROR(CollectInfoInSegment(context, opId, segDirName, indexPath,
                                                                     packageFileTagConfigList, mergePackageMeta));
            }
        }
    }
    return Status::OK();
}

Status MergedSegmentMoveOperation::MergeSegmentToPackageFormat(
    const framework::IndexTaskContext& context, const std::string& segDirName,
    std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec)
{
    auto mergeConfig = context.GetMergeConfig();
    const indexlib::file_system::PackageFileTagConfigList& packageFileTagConfigList =
        mergeConfig.GetPackageFileTagConfigList();
    std::string packageFileTagConfigListStr = autil::legacy::ToJsonString(packageFileTagConfigList);
    AUTIL_LOG(INFO, "package file tag config list[%s]", packageFileTagConfigListStr.c_str());

    std::shared_ptr<indexlib::file_system::IDirectory> outputSegmentDirectory;
    RETURN_STATUS_DIRECTLY_IF_ERROR(GetOrMakeOutputDirectory(context, segDirName, &outputSegmentDirectory));
    ASSIGN_OR_RETURN(std::shared_ptr<indexlib::file_system::IDirectory> currentOpFenceDir,
                     context.GetOpFenceRoot(_desc.GetId(), /*useOpFenceDir=*/true));

    indexlib::file_system::MergePackageMeta mergePackageMeta;
    RETURN_STATUS_DIRECTLY_IF_ERROR(
        CollectFileSizesAndDir(context, segDirName, packageFileTagConfigList, &mergePackageMeta));

    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();

    return indexlib::file_system::MergePackageUtil::ConvertDirToPackage(
               /*workingDirectory=*/currentOpFenceDir,
               /*outputDirectory=*/outputSegmentDirectory, /*parentDirName=*/segDirName, mergePackageMeta,
               mergeConfig.GetPackageFileSizeThresholdBytes(), mergeConfig.GetMergePackageThreadCount())
        .Status();
}

// Get indexConfig and maybe truncate indexConfigs
std::vector<std::shared_ptr<config::IIndexConfig>>
MergedSegmentMoveOperation::GetAllIndexConfigs(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                               const std::string& indexType, const std::string& indexName)
{
    std::shared_ptr<config::IIndexConfig> indexConfig = schema->GetIndexConfig(indexType, indexName);
    if (indexConfig == nullptr && indexType == indexlib::index::INVERTED_INDEX_TYPE_STR) {
        indexConfig = config::InvertedIndexConfig::GetShardingIndexConfig(*schema, indexName);
    }
    assert(indexConfig != nullptr);
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs {indexConfig};
    auto invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
    if (invertedIndexConfig == nullptr) {
        return indexConfigs;
    }
    for (const auto& truncateIndexConfig : invertedIndexConfig->GetTruncateIndexConfigs()) {
        indexConfigs.emplace_back(truncateIndexConfig);
    }
    return indexConfigs;
}

Status MergedSegmentMoveOperation::CollectMetrics(const framework::IndexTaskContext& context,
                                                  const std::string& segDirName,
                                                  std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec)
{
    std::string opToIndexDirStr;
    if (_desc.GetParameter(PARAM_OP_TO_INDEX_DIR, opToIndexDirStr)) {
        // legacy code, for old merge, TODO: by yijie.zhang delete
        std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexDir;
        autil::legacy::FromJsonString(opToIndexDir, opToIndexDirStr);
        for (const auto& opDirPair : opToIndexDir) {
            RETURN_STATUS_DIRECTLY_IF_ERROR(CollectSegmentMetrics(context, /*opId=*/opDirPair.first, segDirName,
                                                                  /*indexPath=*/opDirPair.second.second,
                                                                  segmentMetricsVec));
        }
        return Status::OK();
    }
    if (!_desc.GetParameter(PARAM_OP_TO_INDEX, opToIndexDirStr)) {
        return Status::InternalError("no op to index dir param found");
    }
    std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexs;
    autil::legacy::FromJsonString(opToIndexs, opToIndexDirStr);
    auto schema = context.GetTabletSchema();
    for (const auto& [opId, kvItem] : opToIndexs) {
        const auto& [indexType, indexName] = kvItem;
        auto indexConfig = schema->GetIndexConfig(indexType, indexName);
        for (const auto& innerIndexConfig : GetAllIndexConfigs(schema, indexType, indexName)) {
            std::vector<std::string> indexPaths = innerIndexConfig->GetIndexPath();
            for (const auto& indexPath : indexPaths) {
                RETURN_STATUS_DIRECTLY_IF_ERROR(
                    CollectSegmentMetrics(context, opId, segDirName, indexPath, segmentMetricsVec));
            }
        }
    }
    return Status::OK();
}

Status MergedSegmentMoveOperation::MergeSegment(const framework::IndexTaskContext& context,
                                                const std::string& segDirName,
                                                std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec)
{
    auto schema = context.GetTabletSchema();
    auto status = CollectMetrics(context, segDirName, segmentMetricsVec);
    RETURN_IF_STATUS_ERROR(status, "collect metrics failed [%s]", status.ToString().c_str());
    auto mergeConfig = context.GetMergeConfig();
    if (mergeConfig.IsPackageFileEnabled()) {
        return MergeSegmentToPackageFormat(context, segDirName, segmentMetricsVec);
    }
    std::string opToIndexDirStr;
    if (_desc.GetParameter(PARAM_OP_TO_INDEX_DIR, opToIndexDirStr)) {
        // legacy code, for old merge, TODO: by yijie.zhang delete
        std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexDir;
        autil::legacy::FromJsonString(opToIndexDir, opToIndexDirStr);
        for (const auto& opDirPair : opToIndexDir) {
            auto status = MoveIndexDir(context, /*opId=*/opDirPair.first, segDirName,
                                       /*indexPath=*/opDirPair.second.second);
            if (!status.IsOK()) {
                return status;
            }
        }
        return Status::OK();
    }

    bool hasMovedIndex = false;
    if (_desc.GetParameter(PARAM_OP_TO_INDEX, opToIndexDirStr)) {
        hasMovedIndex = true;
        std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndexs;
        autil::legacy::FromJsonString(opToIndexs, opToIndexDirStr);
        for (const auto& [opId, kvItem] : opToIndexs) {
            const auto& [indexType, indexName] = kvItem;
            auto indexConfig = schema->GetIndexConfig(indexType, indexName);
            auto moveIndexFunc = [&](const std::shared_ptr<config::IIndexConfig>& config,
                                     framework::IndexOperationId opId) -> Status {
                auto indexPaths = config->GetIndexPath();
                for (const auto& indexPath : indexPaths) {
                    auto status = MoveIndexDir(context, opId, segDirName, indexPath);
                    if (!status.IsOK()) {
                        return status;
                    }
                }
                return Status::OK();
            };
            for (const auto& innerIndexConfig : GetAllIndexConfigs(schema, indexType, indexName)) {
                RETURN_IF_STATUS_ERROR(moveIndexFunc(innerIndexConfig, opId), "move index[%s] failed",
                                       innerIndexConfig->GetIndexName().c_str());
            }
        }
    }
    if (!hasMovedIndex) {
        AUTIL_LOG(ERROR, "get op to index dir from desc failed");
        return Status::Corruption("get op to index dir from desc failed");
    }

    return Status::OK();
}

Status MergedSegmentMoveOperation::Execute(const framework::IndexTaskContext& context)
{
    RETURN_IF_STATUS_ERROR(context.RelocateAllDependOpFolders(), "%s", "relocate depend op global root failed");
    std::string segDirName;
    Status status;
    if (!_desc.GetParameter(PARAM_TARGET_SEGMENT_DIR, segDirName)) {
        status = Status::Corruption("get target segment dir from desc failed");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    std::vector<indexlib::framework::SegmentMetrics> segmentMetricsVec;
    RETURN_IF_STATUS_ERROR(MergeSegment(context, segDirName, &segmentMetricsVec),
                           "merge segment use op fence dir failed");
    framework::SegmentInfo targetSegmentInfo;
    std::string segInfoStr;
    if (!_desc.GetParameter(PARAM_TARGET_SEGMENT_INFO, segInfoStr)) {
        status = Status::Corruption("get target segment info from desc failed");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    autil::legacy::FromJsonString(targetSegmentInfo, segInfoStr);
    // Making targetSegmentDir is necessary, because the MergeSegment() above only handles the index dirs inside the
    // segment and entry table is not complete throught this operation.
    std::shared_ptr<indexlib::file_system::IDirectory> targetSegmentDirectory;
    RETURN_STATUS_DIRECTLY_IF_ERROR(GetOrMakeOutputDirectory(context, segDirName, &targetSegmentDirectory));

    indexlib::framework::SegmentMetrics segmentMetrics;
    if (segmentMetricsVec.size() > 1) {
        RETURN_IF_STATUS_ERROR(MergeSegmentMetrics(segmentMetricsVec, &segmentMetrics), "merge segment metrics failed");
    } else if (segmentMetricsVec.size() == 1) {
        segmentMetrics = segmentMetricsVec[0];
    }
    size_t keyCount = 0;
    if (segmentMetrics.GetKeyCount(keyCount)) {
        targetSegmentInfo.docCount = keyCount;
    }

    status = Publish(context, segDirName, indexlib::SEGMETN_METRICS_FILE_NAME, segmentMetrics.ToString());
    RETURN_IF_STATUS_ERROR(status, "publish %s failed.", indexlib::SEGMETN_METRICS_FILE_NAME);

    // TODO(yijie.zhang): fill counter map content
    status = Publish(context, segDirName, COUNTER_FILE_NAME, "");
    RETURN_IF_STATUS_ERROR(status, "publish %s failed.", COUNTER_FILE_NAME.c_str());

    std::string segmentInfoContent;
    std::tie(status, segmentInfoContent) = indexlib::file_system::JsonUtil::ToString(targetSegmentInfo).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "%s to str failed.", SEGMENT_INFO_FILE_NAME.c_str());
    status = Publish(context, segDirName, SEGMENT_INFO_FILE_NAME, segmentInfoContent);
    RETURN_IF_STATUS_ERROR(status, "publish %s failed.", SEGMENT_INFO_FILE_NAME.c_str());

    return status;
}

Status MergedSegmentMoveOperation::CollectSegmentMetrics(
    const framework::IndexTaskContext& context, framework::IndexOperationId opId, const std::string& segDirName,
    const std::string& indexPath, std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec)
{
    auto fenceRootDirectory = context.GetDependOperationFenceRoot(opId);
    if (!_desc.UseOpFenceDir()) {
        std::shared_ptr<indexlib::file_system::IDirectory> fenceSegmentDirectory = nullptr;
        RETURN_STATUS_DIRECTLY_IF_ERROR(GetFenceSegmentDirectory(context, opId, segDirName, &fenceSegmentDirectory));
        std::string indexMetricRoot = PathUtil::JoinPath(std::string(SEGMENT_METRICS_TMP_PATH), indexPath);
        auto [metricStatus, metricExist] = fenceSegmentDirectory->IsExist(indexMetricRoot).StatusWith();
        RETURN_STATUS_DIRECTLY_IF_ERROR(metricStatus);
        if (!metricExist) {
            return Status::OK();
        }
        auto [status, metricDirectory] = fenceSegmentDirectory->GetDirectory(indexMetricRoot).StatusWith();
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        indexlib::framework::SegmentMetrics metrics;
        RETURN_IF_STATUS_ERROR(metrics.LoadSegmentMetrics(metricDirectory), "load segment metrics failed");
        segmentMetricsVec->push_back(metrics);
        return Status::OK();
    }
    std::string indexMetricRoot = indexlib::util::PathUtil::JoinPath(std::string(SEGMENT_METRICS_TMP_PATH), segDirName);
    auto [metricStatus, metricExist] = fenceRootDirectory->IsExist(indexMetricRoot).StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(metricStatus);
    if (!metricExist) {
        return Status::OK();
    }
    auto [status, metricDirectory] = fenceRootDirectory->GetDirectory(indexMetricRoot).StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    indexlib::framework::SegmentMetrics metrics;
    RETURN_IF_STATUS_ERROR(metrics.LoadSegmentMetrics(metricDirectory), "load segment metrics failed");
    segmentMetricsVec->push_back(metrics);
    return Status::OK();
}

// Get or create directory in output storage, under root without fence.
Status
MergedSegmentMoveOperation::GetOrMakeOutputDirectory(const framework::IndexTaskContext& context,
                                                     const std::string& targetPath,
                                                     std::shared_ptr<indexlib::file_system::IDirectory>* outputDir)
{
    indexlib::file_system::DirectoryOption directoryOption;
    directoryOption.recursive = true;
    auto [status, targetDir] =
        context.GetIndexRoot()->GetIDirectory()->MakeDirectory(targetPath, directoryOption).StatusWith();
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    *outputDir = targetDir;
    return Status::OK();
}

Status MergedSegmentMoveOperation::MoveIndexDir(const framework::IndexTaskContext& context,
                                                framework::IndexOperationId opId, const std::string& segDirName,
                                                const std::string& indexPath)
{
    auto rootDirectory = context.GetIndexRoot()->GetIDirectory();
    auto fenceRootDirectory = context.GetDependOperationFenceRoot(opId);
    std::shared_ptr<indexlib::file_system::IDirectory> fenceSegmentDirectory = nullptr;
    RETURN_STATUS_DIRECTLY_IF_ERROR(GetFenceSegmentDirectory(context, opId, segDirName, &fenceSegmentDirectory));

    auto [existStatus, exist] = fenceSegmentDirectory->IsExist(indexPath).StatusWith();
    RETURN_IF_STATUS_ERROR(existStatus, "is exist for [%s] failed", indexPath.c_str());
    if (!exist) {
        // may be moved already
        return Status::OK();
    }
    std::string physicalPath = fenceSegmentDirectory->GetPhysicalPath(indexPath);
    indexlib::file_system::FenceContextPtr fenceContext = rootDirectory->GetFenceContext();
    std::string targetPath = PathUtil::JoinPath(segDirName, indexPath);
    auto status = toStatus(rootDirectory->GetFileSystem()->MergeDirs(
        {physicalPath}, targetPath, indexlib::file_system::MergeDirsOption::NoMergePackage()));
    if (status.IsOK() || status.IsExist()) {
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(status, "merge index path [%s] failed", physicalPath.c_str());
    return status;
}

Status MergedSegmentMoveOperation::MergeSegmentMetrics(
    const std::vector<indexlib::framework::SegmentMetrics>& segmentMetricsVec,
    indexlib::framework::SegmentMetrics* segmentMetrics)
{
    for (const auto& indexSegmentMetrics : segmentMetricsVec) {
        if (!segmentMetrics->MergeSegmentMetrics(indexSegmentMetrics)) {
            return Status::Corruption("merge segment metrics failed");
        }
    }
    return Status::OK();
}

}} // namespace indexlibv2::table
