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
#include "indexlib/file_system/package/MergePackageMeta.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
namespace indexlibv2::framework {
class Version;
class IndexTaskResourceManager;
} // namespace indexlibv2::framework
namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class Version;
}

namespace indexlibv2::table {

class MergedVersionCommitOperation : public framework::IndexOperation
{
public:
    static constexpr char OPERATION_TYPE[] = "merged_version_commit";
    static constexpr char PATCH_INDEX_DIR[] = "patch_index_dir";
    static constexpr char DROP_INDEX_INFO[] = "drop_index_info";
    struct DropIndexInfo : public autil::legacy::Jsonizable {
        void Jsonize(JsonWrapper& json) override
        {
            json.Jsonize("segment_id", segId);
            json.Jsonize("index_infos", indexInfos);
        }
        segmentid_t segId;
        std::vector<std::pair<std::string, std::string>> indexInfos; //[(indexType, indexName)...]
    };

public:
    MergedVersionCommitOperation(const framework::IndexOperationDescription& desc);
    ~MergedVersionCommitOperation();

    Status Execute(const framework::IndexTaskContext& context) override;

    static framework::IndexOperationDescription
    CreateOperationDescription(framework::IndexOperationId opId, const framework::Version& version,
                               const std::string& patchIndexDir, const std::vector<DropIndexInfo>& dropIndexes);

private:
    Status MountAndMaybePackageResourceDir(const framework::IndexTaskContext& context,
                                           const std::string& resourceDirName);
    Status MountResourceDir(const std::shared_ptr<indexlib::file_system::Directory>& indexRoot,
                            const std::string& resourceName);
    Status MountPatchIndexDir(const std::shared_ptr<indexlib::file_system::Directory>& indexRoot);
    Status ValidateVersion(const std::string& indexRootPath, const std::shared_ptr<config::TabletSchema>& schema,
                           versionid_t versionId);
    Status UpdateSegmentDescriptions(const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager,
                                     framework::Version* targetVersion);
    static Status CollectInfoInDir(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                   indexlib::file_system::MergePackageMeta* mergePackageMeta);
    Status ConvertResourceDirToPackage(const framework::IndexTaskContext& context,
                                       const std::shared_ptr<indexlib::file_system::IDirectory>& resourceDir,
                                       const std::string& resourceDirName,
                                       const std::shared_ptr<indexlib::file_system::IDirectory>& currentOpFenceDir);
    Status IsPackageConversionDone(const framework::IndexTaskContext& context, const std::string& resourceDirName);

private:
    framework::IndexOperationDescription _desc;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
