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
#ifndef __INDEXLIB_DEPLOY_FILES_WRAPPER_H
#define __INDEXLIB_DEPLOY_FILES_WRAPPER_H

#include <memory>
#include <vector>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(file_system, LoadConfig);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(config, OnlineConfig);
namespace indexlibv2::config {
class TabletOptions;
};

namespace indexlib { namespace index_base {

class DeployIndexWrapper
{
public:
    DeployIndexWrapper() = delete;
    ~DeployIndexWrapper() = delete;

public:
    struct GetDeployIndexMetaInputParams {
        std::string rawPath;    // raw index partition path, index produced here
        std::string localPath;  // local partition path
        std::string remotePath; // index partition path came from suze target, refer to mpangu, dcache or equal rawPath,
                                // just default value
        const config::OnlineConfig* baseOnlineConfig = nullptr;
        const config::OnlineConfig* targetOnlineConfig = nullptr;
        versionid_t baseVersionId = INVALID_VERSION;
        versionid_t targetVersionId = INVALID_VERSION;
    };
    static bool GetDeployIndexMeta(const GetDeployIndexMetaInputParams& inputParams,
                                   file_system::DeployIndexMetaVec& localDeployIndexMetaVec,
                                   file_system::DeployIndexMetaVec& remoteDeployIndexMetaVec) noexcept;
    static int64_t GetIndexSize(const std::string& rawPath, const versionid_t targetVersion);
    static int64_t GetSegmentSize(const file_system::DirectoryPtr& directory, bool includePatch);
    static std::string GetDeployMetaFileName(versionid_t versionId);
    static std::string GetDeployMetaLockFileName(versionid_t versionId);

public:
    // only for tools
    // for final deploy index meta bind with version, throw exception when failed
    static void DumpDeployMeta(const std::string& partitionPath, const Version& version);
    static void DumpDeployMeta(const file_system::DirectoryPtr& partitionDirectory, const Version& version);

    static bool DumpTruncateMetaIndex(const file_system::DirectoryPtr& dir);
    static bool DumpAdaptiveBitmapMetaIndex(const file_system::DirectoryPtr& dir);

    static bool DumpSegmentDeployIndex(const file_system::DirectoryPtr& segmentDir, const std::string& lifecycle);
    static void DumpSegmentDeployIndex(const file_system::DirectoryPtr& directory, const SegmentInfoPtr& segmentInfo);
    // Note: fileList is physical file list
    static bool DumpSegmentDeployIndex(const file_system::DirectoryPtr& directory, const fslib::FileList& fileList);

public:
    static bool GenerateDisableLoadConfig(const std::string& rootPath, versionid_t versionId,
                                          file_system::LoadConfig& loadConfig);
    static bool GenerateDisableLoadConfig(const indexlibv2::config::TabletOptions& tabletOptions,
                                          file_system::LoadConfig& loadConfig);

public:
    static bool TEST_GetSegmentSize(const index_base::SegmentData& segmentData, bool includePatch,
                                    int64_t& totalLength);
    static bool TEST_DumpSegmentDeployIndex(const std::string& segmentPath, const fslib::FileList& fileList);

private:
    friend class DeployIndexWrapperTest;
};

DEFINE_SHARED_PTR(DeployIndexWrapper);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_DEPLOY_FILES_WRAPPER_H
