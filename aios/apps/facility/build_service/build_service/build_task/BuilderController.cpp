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
#include "build_service/build_task/BuilderController.h"

#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/IndexPathConstructor.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/table/common/CommonVersionImporter.h"

namespace build_service::build_task {

BS_LOG_SETUP(build_task, BuilderController);

indexlib::Status BuilderController::loadVersion(const proto::VersionProgress& vp, bool isFullBuild,
                                                indexlibv2::framework::Version& version) const
{
    config::BuildServiceConfig serviceConfig;
    if (!_initParam.resourceReader->getConfigWithJsonPath(config::BUILD_APP_FILE_NAME, "", serviceConfig)) {
        BS_LOG(ERROR, "failed to parse build_app.json, config path:%s",
               _initParam.resourceReader->getConfigPath().c_str());
        return indexlib::Status::InternalError();
    }
    const std::string builderIndexRoot = serviceConfig.getBuilderIndexRoot(isFullBuild);
    const std::string indexPath = util::IndexPathConstructor::constructIndexPath(builderIndexRoot, _initParam.pid);
    const std::string fencePath = indexlibv2::PathUtil::JoinPath(indexPath, vp.fence);

    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(fencePath);
    auto [status, v] = indexlibv2::framework::VersionLoader::GetVersion(rootDir, vp.versionId);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "load version [%s][%d] failed", fencePath.c_str(), vp.versionId);
        return status;
    }
    version = *v;
    return status;
}

bool BuilderController::handleTarget(const config::TaskTarget& target)
{
    std::string content;
    if (!target.getTargetDescription(config::BS_BUILD_TASK_TARGET, content)) {
        BS_LOG(ERROR, "get build task target description failed");
        return false;
    }

    auto [status, buildTaskInfo] =
        indexlib::file_system::JsonUtil::FromString<proto::BuildTaskTargetInfo>(content).StatusWith();
    if (!status.IsOK()) {
        BS_LOG(ERROR, "parse build task target info failed.");
        return false;
    }

    const bool isFullBuild = buildTaskInfo.buildStep == "full";
    std::vector<indexlibv2::framework::Version> versions;
    const std::vector<proto::VersionProgress>& progress = buildTaskInfo.slaveVersionProgress;
    if (progress.empty()) {
        _isDone = buildTaskInfo.finished;
        return true;
    }
    for (const auto& vp : progress) {
        indexlibv2::framework::Version version;
        auto status = loadVersion(vp, isFullBuild, version);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "load version [%s][%d] failed.", vp.fence.c_str(), vp.versionId);
            return false;
        }
        versions.emplace_back(std::move(version));
    }

    indexlibv2::framework::ImportOptions options;
    options.SetImportStrategy(indexlibv2::table::CommonVersionImporter::KEEP_SEGMENT_OVERWRITE_LOCATOR);
    options.SetImportType(indexlibv2::table::CommonVersionImporter::COMMON_TYPE);
    status = _tablet->Import(versions, options);
    if (!status.IsOK()) {
        _hasFatalError = true;
        BS_LOG(ERROR, "import version failed");
        return false;
    }

    _isDone = buildTaskInfo.finished;
    return true;
}

bool BuilderController::hasFatalError() const { return _hasFatalError; }

bool BuilderController::isDone() const { return _isDone; }

} // namespace build_service::build_task
