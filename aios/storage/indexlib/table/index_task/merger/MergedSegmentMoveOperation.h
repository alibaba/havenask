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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/package/MergePackageMeta.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
namespace indexlibv2::framework {
class SegmentInfo;
}

namespace indexlibv2::table {

class MergedSegmentMoveOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "merged_segment_move";
    static constexpr char PARAM_TARGET_SEGMENT_DIR[] = "target_segment_dir";
    static constexpr char PARAM_TARGET_SEGMENT_INFO[] = "segment_info";
    static constexpr char PARAM_OP_TO_INDEX_DIR[] = "op_to_index_dir";
    static constexpr char PARAM_OP_TO_INDEX[] = "op_to_index";

public:
    MergedSegmentMoveOperation(const framework::IndexOperationDescription& desc);
    ~MergedSegmentMoveOperation();

    Status Execute(const framework::IndexTaskContext& context) override;

private:
    Status MoveIndexDir(const framework::IndexTaskContext& context, framework::IndexOperationId opId,
                        const std::string& segDirName, const std::string& indexPath);
    Status MergeSegmentMetrics(const std::vector<indexlib::framework::SegmentMetrics>& segMetricsVec,
                               indexlib::framework::SegmentMetrics* segmentMetrics);
    Status MergeSegment(const framework::IndexTaskContext& context, const std::string& segDirName,
                        std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec);

    Status CollectMetrics(const framework::IndexTaskContext& context, const std::string& segDirName,
                          std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec);
    Status CollectSegmentMetrics(const framework::IndexTaskContext& context, framework::IndexOperationId opId,
                                 const std::string& segDirName, const std::string& indexPath,
                                 std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec);
    Status CollectFileSizesAndDir(const framework::IndexTaskContext& context, const std::string& segDirName,
                                  const indexlib::file_system::PackageFileTagConfigList& packageFileTagConfigList,
                                  indexlib::file_system::MergePackageMeta* mergePackageMeta);
    static Status GetOrMakeOutputDirectory(const framework::IndexTaskContext& context, const std::string& targetPath,
                                           std::shared_ptr<indexlib::file_system::IDirectory>* outputDir);
    static Status GetFenceSegmentDirectory(const framework::IndexTaskContext& context, framework::IndexOperationId opId,
                                           const std::string& segDirName,
                                           std::shared_ptr<indexlib::file_system::IDirectory>* segmentDirectory);
    static Status CollectInfoInSegment(const framework::IndexTaskContext& context, framework::IndexOperationId opId,
                                       const std::string& segDirName, const std::string& indexPath,
                                       const indexlib::file_system::PackageFileTagConfigList& packageFileTagConfigList,
                                       indexlib::file_system::MergePackageMeta* mergePackageMeta);
    Status MergeSegmentToPackageFormat(const framework::IndexTaskContext& context, const std::string& segDirName,
                                       std::vector<indexlib::framework::SegmentMetrics>* segmentMetricsVec);
    static std::vector<std::shared_ptr<config::IIndexConfig>>
    GetAllIndexConfigs(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, const std::string& indexType,
                       const std::string& indexName);

private:
    framework::IndexOperationDescription _desc;
    static constexpr std::array<const char*, 3> SKIP_PACKAGING_SEGMENT_FILE_LIST = {"segment_info", "segment_metrics",
                                                                                    "counter"};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
