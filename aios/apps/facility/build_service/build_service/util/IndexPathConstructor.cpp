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
#include "build_service/util/IndexPathConstructor.h"

#include <algorithm>

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
namespace build_service { namespace util {

BS_LOG_SETUP(util, IndexPathConstructor);

const string IndexPathConstructor::GENERATION_PREFIX = "generation_";
const string IndexPathConstructor::PARTITION_PREFIX = "partition_";
const string IndexPathConstructor::DOC_RECLAIM_DATA_DIR = "doc_reclaim_data";

IndexPathConstructor::IndexPathConstructor() {}

IndexPathConstructor::~IndexPathConstructor() {}

std::string IndexPathConstructor::constructSyncRtIndexPath(const std::string& indexRoot, const std::string& clusterName,
                                                           const std::string& generationId,
                                                           const std::string& rangeFrom, const std::string& rangeTo,
                                                           const std::string& remoteRtDir)
{
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + generationId);
    paths.push_back(remoteRtDir);
    paths.push_back(PARTITION_PREFIX + rangeFrom + "_" + rangeTo);
    return StringUtil::toString(paths, "/") + "/";
}

bool IndexPathConstructor::parsePartitionRange(const std::string& partitionPath, uint32_t& rangeFrom, uint32_t& rangeTo)
{
    string partPath = partitionPath;
    auto iter = partPath.end();
    while (*iter == '/') {
        partPath.erase(iter);
        iter = partPath.end();
    }
    string partitionName;
    string dir;
    fslib::util::FileUtil::splitFileName(partPath, dir, partitionName);
    vector<string> partVec = StringUtil::split(partitionName, "_");
    if (partVec.size() != 3) {
        BS_LOG(ERROR, "invalid partition path[%s]", partitionPath.c_str());
        return false;
    }
    if (partVec[0] != "partition") {
        BS_LOG(ERROR, "invalid partition path[%s]", partitionPath.c_str());
        return false;
    }
    if (!StringUtil::fromString(partVec[1], rangeFrom)) {
        BS_LOG(ERROR, "invalid partition path[%s]", partitionPath.c_str());
        return false;
    }
    if (!StringUtil::fromString(partVec[2], rangeTo)) {
        BS_LOG(ERROR, "invalid partition path[%s]", partitionPath.c_str());
        return false;
    }
    if (rangeFrom >= rangeTo) {
        BS_LOG(ERROR, "invalid partition path[%s]", partitionPath.c_str());
        return false;
    }
    return true;
}

string IndexPathConstructor::getGenerationId(const string& indexRoot, const string& clusterName,
                                             const string& buildMode, const string& rangeFrom, const string& rangeTo)
{
    vector<generationid_t> generations = getGenerationList(indexRoot, clusterName);
    if (buildMode == BUILD_MODE_FULL) {
        generationid_t generationId = 0;
        if (!generations.empty()) {
            generationId = generations.back() + 1;
        }
        return StringUtil::toString(generationId);
    }
    for (vector<generationid_t>::reverse_iterator it = generations.rbegin(); it != generations.rend(); ++it) {
        if (isGenerationContainRange(indexRoot, clusterName, *it, rangeFrom, rangeTo)) {
            return StringUtil::toString(*it);
        }
    }

    // partition range not consistent with full
    string errorMsg =
        "cluster [" + clusterName + "] partition range [" + rangeFrom + "_" + rangeTo + "] not consistent with full";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return "";
}

string IndexPathConstructor::constructIndexPath(const string& indexRoot, const proto::PartitionId& partitionId)
{
    string clusterName = partitionId.clusternames(0);
    string generationIdStr = StringUtil::toString(partitionId.buildid().generationid());
    string rangeFrom = StringUtil::toString(partitionId.range().from());
    string rangeTo = StringUtil::toString(partitionId.range().to());
    return IndexPathConstructor::constructIndexPath(indexRoot, clusterName, generationIdStr, rangeFrom, rangeTo);
}

string IndexPathConstructor::constructGenerationPath(const string& indexRoot, const proto::PartitionId& partitionId)
{
    string clusterName = partitionId.clusternames(0);
    return constructGenerationPath(indexRoot, clusterName, partitionId.buildid().generationid());
}

string IndexPathConstructor::constructGenerationPath(const string& indexRoot, const string& clusterName,
                                                     generationid_t generationId)
{
    string generationIdStr = StringUtil::toString(generationId);
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + generationIdStr);
    return StringUtil::toString(paths, "/") + "/";
}

string IndexPathConstructor::constructIndexPath(const string& indexRoot, const string& clusterName,
                                                const string& generationId, const string& rangeFrom,
                                                const string& rangeTo)
{
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + generationId);
    paths.push_back(PARTITION_PREFIX + rangeFrom + "_" + rangeTo);
    return StringUtil::toString(paths, "/") + "/";
}

bool IndexPathConstructor::isGenerationContainRange(const string& indexRoot, const string& clusterName,
                                                    generationid_t generationId, const string& rangeFrom,
                                                    const string& rangeTo)
{
    string partitionPath =
        constructIndexPath(indexRoot, clusterName, StringUtil::toString(generationId), rangeFrom, rangeTo);
    bool dirExist = false;
    return fslib::util::FileUtil::isDir(partitionPath, dirExist) && dirExist;
}

vector<generationid_t> IndexPathConstructor::getGenerationList(const string& indexRoot, const string& clusterName)
{
    string clusterIndexPath = fslib::util::FileUtil::joinFilePath(indexRoot, clusterName);
    vector<string> fileList;
    if (!fslib::util::FileUtil::listDir(clusterIndexPath, fileList)) {
        string errorMsg = "list cluster index path " + clusterIndexPath + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return vector<generationid_t>();
    }

    vector<generationid_t> generations;
    for (size_t i = 0; i < fileList.size(); i++) {
        bool dirExist = false;
        if (!fslib::util::FileUtil::isDir(fslib::util::FileUtil::joinFilePath(clusterIndexPath, fileList[i]),
                                          dirExist) ||
            !dirExist) {
            continue;
        }
        size_t pos = fileList[i].find(GENERATION_PREFIX);
        if (0 != pos) {
            continue;
        }
        string generationSuffix = fileList[i].substr(GENERATION_PREFIX.size());
        string::iterator it = generationSuffix.end();
        --it;
        if (*it == '/') {
            generationSuffix.erase(it);
        }
        generationid_t currentGenerationId;
        if (StringUtil::strToInt32(generationSuffix.c_str(), currentGenerationId) && currentGenerationId >= 0) {
            generations.push_back(currentGenerationId);
        }
    }

    sort(generations.begin(), generations.end());
    return generations;
}

generationid_t IndexPathConstructor::getNextGenerationId(const string& indexRoot, const string& clusterName)
{
    vector<generationid_t> generationIds = getGenerationList(indexRoot, clusterName);
    if (generationIds.empty()) {
        return generationid_t(0);
    } else {
        return generationIds.back() + 1;
    }
}

generationid_t IndexPathConstructor::getNextGenerationId(const string& indexRoot, const vector<string>& clusters)
{
    generationid_t generationId = 0;
    for (size_t i = 0; i < clusters.size(); i++) {
        generationId = max(generationId, getNextGenerationId(indexRoot, clusters[i]));
    }
    return generationId;
}

string IndexPathConstructor::getGenerationIndexPath(const string& indexRoot, const string& clusterName,
                                                    generationid_t generationId)
{
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + StringUtil::toString(generationId));
    return StringUtil::toString(paths, "/") + "/";
}

string IndexPathConstructor::getMergerCheckpointPath(const std::string& indexRoot,
                                                     const proto::PartitionId& partitionId)
{
    string indexPath = constructIndexPath(indexRoot, partitionId);
    string generationPath = fslib::util::FileUtil::getParentDir(indexPath);
    string roleId;
    ProtoUtil::partitionIdToRoleId(partitionId, roleId);
    return fslib::util::FileUtil::joinFilePath(generationPath, roleId + ".checkpoint");
}

string IndexPathConstructor::constructDocReclaimDirPath(const std::string& indexRoot,
                                                        const proto::PartitionId& partitionId)
{
    string generationPath = constructGenerationPath(indexRoot, partitionId);
    return fslib::util::FileUtil::joinFilePath(generationPath, DOC_RECLAIM_DATA_DIR);
}

string IndexPathConstructor::constructDocReclaimDirPath(const std::string& indexRoot, const std::string& clusterName,
                                                        generationid_t generationId)
{
    string generationPath = constructGenerationPath(indexRoot, clusterName, generationId);
    return fslib::util::FileUtil::joinFilePath(generationPath, DOC_RECLAIM_DATA_DIR);
}

}} // namespace build_service::util
