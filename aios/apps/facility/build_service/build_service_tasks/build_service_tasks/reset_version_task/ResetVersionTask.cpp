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
#include "build_service_tasks/reset_version_task/ResetVersionTask.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionCleaner.h"
#include "indexlib/framework/VersionLoader.h"

using namespace std;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, ResetVersionTask);

ResetVersionTask::ResetVersionTask() {}

ResetVersionTask::~ResetVersionTask() {}

bool ResetVersionTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    _buildId.set_datatable(_taskInitParam.buildId.dataTable);
    _buildId.set_generationid(_taskInitParam.buildId.generationId);
    _buildId.set_appname(_taskInitParam.buildId.appName);
    _clusterName = initParam.clusterName;
    return true;
}
bool ResetVersionTask::isDone(const config::TaskTarget& target) { return _isFinished; }
indexlib::util::CounterMapPtr ResetVersionTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }
string ResetVersionTask::getTaskStatus() { return _isFinished ? "CleanFinish" : "Cleaning"; }
void ResetVersionTask::handleFatalError() { BS_LOG(WARN, "unexpected error happened. retry handleTarget later"); }
bool ResetVersionTask::handleTarget(const config::TaskTarget& target)
{
    if (_isFinished) {
        return true;
    }
    if (!fetchParamFromTaskTarget(target)) {
        string errorMsg = "task params is invalid";
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_ERROR_CONFIG, errorMsg, proto::BS_STOP);
        return false;
    }
    if (!constructPartitionIndexRoot()) {
        string errorMsg = "constructPartitionIndexRoot failed";
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_ERROR_CONFIG, errorMsg, proto::BS_STOP);
        return false;
    }
    if (!validateVersionInAllPartition()) {
        string errorMsg = "input versionId is invalid";
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_ERROR_CONFIG, errorMsg, proto::BS_STOP);
        return false;
    }
    if (!cleanIndex()) {
        string errorMsg = "clean index failed";
        REPORT_ERROR_WITH_ADVICE(proto::SERVICE_ERROR_CONFIG, errorMsg, proto::BS_STOP);
        return false;
    }
    _isFinished = true;
    BS_LOG(INFO, "Done clone index task");
    return true;
}
bool ResetVersionTask::fetchParamFromTaskTarget(const config::TaskTarget& target)
{
    if (_clusterName.empty()) {
        BS_LOG(ERROR, "does not have clusterName");
        return false;
    }
    std::string versionIdStr, partitionCountStr;
    auto getTargetDesc = [&target](const std::string& key, std::string& value) -> bool {
        if (!target.getTargetDescription(key, value)) {
            BS_LOG(ERROR, "missing [%s]", key.c_str());
            return false;
        }
        return true;
    };

    if ((!getTargetDesc("indexRoot", _indexRoot)) || (!getTargetDesc("versionId", versionIdStr)) ||
        (!getTargetDesc("partitionCount", partitionCountStr))) {
        return false;
    }
    _versionId = autil::StringUtil::fromString<indexlib::versionid_t>(versionIdStr);
    _partitionCount = autil::StringUtil::fromString<uint32_t>(partitionCountStr);
    return true;
}

bool ResetVersionTask::constructPartitionIndexRoot()
{
    _partitionDir.clear();
    if (_partitionCount == 0) {
        BS_LOG(ERROR, "partitionCount [%d] is invalid", _partitionCount);
        return false;
    }
    vector<proto::Range> ranges = util::RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, _partitionCount);
    for (const auto& range : ranges) {
        string expectPartDir = util::IndexPathConstructor::constructIndexPath(
            _indexRoot, _clusterName, autil::StringUtil::toString(_buildId.generationid()),
            autil::StringUtil::toString(range.from()), autil::StringUtil::toString(range.to()));
        _partitionDir.push_back(expectPartDir);
    }
    return true;
}

bool ResetVersionTask::validateVersionInAllPartition() const
{
    // check if versionId exist
    for (const auto& partDir : _partitionDir) {
        std::string versionPath = partDir + "/version." + autil::StringUtil::toString(_versionId);
        if (!fslib::util::FileUtil::isExist(versionPath)) {
            BS_LOG(ERROR, "version [%s] is not exist.", versionPath.c_str());
            return false;
        }
    }
    return true;
}

bool ResetVersionTask::cleanObsoleteSegments(const std::shared_ptr<indexlib::file_system::Directory>& rootDir) const
{
    indexlibv2::framework::Version version;
    auto status = indexlibv2::framework::VersionLoader::LoadVersion(
        rootDir, indexlibv2::PathUtil::GetVersionFileName(_versionId), &version);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "directory [%s] fail to load version [%d]", rootDir->DebugString().c_str(), _versionId);
        return false;
    }
    // get max merged segmentId
    auto maxMergeSegId = indexlibv2::INVALID_SEGMENTID;
    for (size_t i = 0; i < version.GetSegmentCount(); i++) {
        const auto& segId = version[i];
        if (indexlibv2::framework::Segment::IsMergedSegmentId(segId)) {
            maxMergeSegId = std::max(maxMergeSegId, segId);
        }
    }

    // if root path has segments whose id > maxMergeSegId, indicating that the reset version task may failover and clean
    // is not completed in the last run, e.g., version is cleaned but segments are not.
    std::vector<std::string> segmentsToRemove;
    fslib::FileList segList;
    status = indexlibv2::framework::VersionLoader::ListSegment(rootDir, &segList);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "list segment [%s] failed", rootDir->DebugString().c_str());
        return false;
    }
    for (const auto& segName : segList) {
        auto segId = indexlibv2::PathUtil::GetSegmentIdByDirName(segName);
        if (segId > maxMergeSegId) {
            segmentsToRemove.push_back(segName);
        }
    }

    BS_LOG(INFO, "directory [%s], found [%s] segments in diretory whose segmentId > max segId [%d] in version [%d]",
           rootDir->DebugString().c_str(), autil::StringUtil::toString(segmentsToRemove).c_str(), maxMergeSegId,
           _versionId);

    for (const auto& segFileName : segmentsToRemove) {
        auto status = rootDir->GetIDirectory()
                          ->RemoveDirectory(segFileName, indexlib::file_system::RemoveOption::MayNonExist())
                          .Status();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "remove segment [%s] failed", segFileName.c_str());
            return false;
        }
        BS_LOG(INFO, "Segment [%s] is removed", segFileName.c_str());
    }
    return true;
}

bool ResetVersionTask::calculateReservedVersion(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                                indexlib::versionid_t resetVersionId,
                                                std::set<indexlibv2::framework::VersionCoord>& reservedVersionSet,
                                                indexlib::versionid_t& maxVersionId) const
{
    reservedVersionSet.clear();
    fslib::FileList versionList;
    auto status = indexlibv2::framework::VersionLoader::ListVersion(rootDir, &versionList);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "fail to list version in directory [%s]", rootDir->DebugString().c_str());
        return false;
    }
    for (const auto& versionFileName : versionList) {
        auto versionId = indexlibv2::framework::VersionLoader::GetVersionId(versionFileName);
        if (versionId <= resetVersionId) {
            reservedVersionSet.insert(indexlibv2::framework::VersionCoord(versionId, ""));
        }
    }
    maxVersionId = indexlibv2::framework::VersionLoader::GetVersionId(versionList.back());
    return true;
}
bool ResetVersionTask::cleanPartitionLevelIndex(const std::string& rootDirStr) const
{
    indexlibv2::framework::VersionCleaner versionCleaner;
    indexlibv2::framework::VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;

    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(rootDirStr);
    std::set<indexlibv2::framework::VersionCoord> reservedVersionSet;
    if (!calculateReservedVersion(rootDir, _versionId, reservedVersionSet, options.currentMaxVersionId)) {
        BS_LOG(ERROR, "calculateReservedVersion failed [%s]", rootDirStr.c_str());
        return false;
    }
    auto status = versionCleaner.Clean(rootDir->GetIDirectory(), options, reservedVersionSet);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "fail to clean directory [%s]", rootDirStr.c_str());
        return false;
    }
    if (!cleanObsoleteSegments(rootDir)) {
        BS_LOG(ERROR, "cleanObsoleteSegments failed");
        return false;
    }
    BS_LOG(INFO, "cleanPartitionLevelIndex done [%s]", rootDirStr.c_str());
    return true;
}
bool ResetVersionTask::cleanIndex() const
{
    for (const auto& partDirStr : _partitionDir) {
        if (!cleanPartitionLevelIndex(partDirStr)) {
            BS_LOG(ERROR, "cleanPartitionLevelIndex failed [%s]", partDirStr.c_str());
            return false;
        }
    }
    return true;
}
}} // namespace build_service::task_base
