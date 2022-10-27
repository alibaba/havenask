#include "build_service/util/IndexPathConstructor.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>
#include <algorithm>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
namespace build_service {
namespace util {

BS_LOG_SETUP(util, IndexPathConstructor);

const string IndexPathConstructor::GENERATION_PREFIX = "generation_";
const string IndexPathConstructor::PARTITION_PREFIX = "partition_";
const string IndexPathConstructor::DOC_RECLAIM_DATA_DIR = "doc_reclaim_data";

IndexPathConstructor::IndexPathConstructor() {
}

IndexPathConstructor::~IndexPathConstructor() {
}

bool IndexPathConstructor::parsePartitionRange(
    const std::string& partitionPath,
    uint32_t& rangeFrom, uint32_t& rangeTo) {
    string partPath = partitionPath;
    auto iter = partPath.end();
    while (*iter == '/') {
        partPath.erase(iter);
        iter = partPath.end();
    }
    string partitionName;
    string dir;
    FileUtil::splitFileName(partPath, dir, partitionName);
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

string IndexPathConstructor::getGenerationId(
        const string &indexRoot, const string &clusterName,
        const string &buildMode, const string &rangeFrom,
        const string &rangeTo)
{
    vector<generationid_t> generations = getGenerationList(indexRoot, clusterName);
    if (buildMode == BUILD_MODE_FULL) {
        generationid_t generationId = 0;
        if (!generations.empty()) {
            generationId = generations.back() + 1;
        }
        return StringUtil::toString(generationId);
    }
    for (vector<generationid_t>::reverse_iterator it = generations.rbegin();
         it != generations.rend(); ++it)
    {
        if (isGenerationContainRange(
                        indexRoot, clusterName, 
                        *it, rangeFrom, rangeTo)) 
        {
            return StringUtil::toString(*it);
        }
    }

    // partition range not consistent with full
    string errorMsg = "cluster [" + clusterName + "] partition range ["
                      + rangeFrom + "_" + rangeTo
                      + "] not consistent with full";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return "";
}

string IndexPathConstructor::constructIndexPath(
        const string &indexRoot,
        const proto::PartitionId &partitionId)
{
    string clusterName = partitionId.clusternames(0);
    string generationIdStr = StringUtil::toString(partitionId.buildid().generationid());
    string rangeFrom = StringUtil::toString(partitionId.range().from());
    string rangeTo = StringUtil::toString(partitionId.range().to());
    return IndexPathConstructor::constructIndexPath(
            indexRoot, clusterName, generationIdStr, rangeFrom, rangeTo);
}

string IndexPathConstructor::constructGenerationPath(
    const string &indexRoot, const proto::PartitionId &partitionId)
{
    string clusterName = partitionId.clusternames(0);
    return constructGenerationPath(indexRoot, clusterName, partitionId.buildid().generationid());
}

string IndexPathConstructor::constructGenerationPath(
    const string &indexRoot, const string &clusterName, generationid_t generationId)
{
    string generationIdStr = StringUtil::toString(generationId);
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + generationIdStr);
    return StringUtil::toString(paths, "/") + "/";
}

string IndexPathConstructor::constructIndexPath(
        const string &indexRoot, const string &clusterName, const string &generationId,
        const string &rangeFrom, const string &rangeTo)
{
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + generationId);
    paths.push_back(PARTITION_PREFIX + rangeFrom + "_" + rangeTo);
    return StringUtil::toString(paths, "/") + "/";
}

bool IndexPathConstructor::isGenerationContainRange(
        const string &indexRoot, const string &clusterName,
        generationid_t generationId, const string &rangeFrom, const string &rangeTo)
{
    string partitionPath = constructIndexPath(indexRoot, clusterName,
            StringUtil::toString(generationId), rangeFrom, rangeTo);
    bool dirExist = false;
    return FileUtil::isDir(partitionPath, dirExist) && dirExist;
}

vector<generationid_t> IndexPathConstructor::getGenerationList(
        const string &indexRoot, const string &clusterName)
{
    string clusterIndexPath = FileUtil::joinFilePath(indexRoot, clusterName);
    vector<string> fileList;
    if (!FileUtil::listDir(clusterIndexPath, fileList)) {
        string errorMsg = "list cluster index path " + clusterIndexPath
                          + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return vector<generationid_t>();
    }

    vector<generationid_t> generations;
    for (size_t i = 0; i < fileList.size(); i++) {
        bool dirExist = false;
        if (!FileUtil::isDir(FileUtil::joinFilePath(clusterIndexPath, fileList[i]), dirExist) || !dirExist) {
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
        if (StringUtil::strToInt32(generationSuffix.c_str(), currentGenerationId)
            && currentGenerationId >= 0)
        {
            generations.push_back(currentGenerationId);
        }
    }

    sort(generations.begin(), generations.end());
    return generations;
}

generationid_t IndexPathConstructor::getNextGenerationId(
        const string &indexRoot, const string &clusterName)
{
    vector<generationid_t> generationIds = getGenerationList(indexRoot, clusterName);
    if (generationIds.empty()) {
        return generationid_t(0);
    } else {
        return generationIds.back() + 1;
    }
}

generationid_t IndexPathConstructor::getNextGenerationId(const string &indexRoot, 
        const vector<string>& clusters)
{
    generationid_t generationId = 0;
    for (size_t i = 0; i < clusters.size(); i++) {
        generationId = max(generationId, getNextGenerationId(indexRoot, clusters[i]));
    }
    return generationId;
}

string IndexPathConstructor::getGenerationIndexPath(const string &indexRoot, 
        const string &clusterName, generationid_t generationId)
{
    vector<string> paths;
    paths.push_back(indexRoot);
    paths.push_back(clusterName);
    paths.push_back(GENERATION_PREFIX + StringUtil::toString(generationId));
    return StringUtil::toString(paths, "/") + "/";
}

string IndexPathConstructor::getMergerCheckpointPath(const std::string &indexRoot,
                               const proto::PartitionId &partitionId)
{
    string indexPath = constructIndexPath(indexRoot, partitionId);
    string generationPath = FileUtil::getParentDir(indexPath);
    string roleId;
    ProtoUtil::partitionIdToRoleId(partitionId, roleId);
    return FileUtil::joinFilePath(generationPath, roleId + ".checkpoint");
}

string IndexPathConstructor::constructDocReclaimDirPath(
        const std::string &indexRoot, const proto::PartitionId &partitionId)
{
    string generationPath = constructGenerationPath(indexRoot, partitionId);
    return FileUtil::joinFilePath(generationPath, DOC_RECLAIM_DATA_DIR);
}

string IndexPathConstructor::constructDocReclaimDirPath(
        const std::string &indexRoot, const std::string &clusterName,
        generationid_t generationId)
{
    string generationPath = constructGenerationPath(indexRoot, clusterName, generationId);
    return FileUtil::joinFilePath(generationPath, DOC_RECLAIM_DATA_DIR);
}


}
}
