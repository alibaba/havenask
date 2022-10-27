#include <autil/StringUtil.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/common/HaPathDefs.h>
#include <ha3/proto/ProtoClassUtil.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace suez::turing;

USE_HA3_NAMESPACE(proto);

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, HaPathDefs);

const string RANGE_PREFIX="partition_";
const string VERSION_PREFIX="generation_";
const string PATH_NAME_SEPRATOR="_";
const string INCREMENTAL_VERSION_PREFIX="version.";

bool HaPathDefs::configPathToVersion(const string &configPath,
                                     int32_t &version)
{
    const vector<string> &names = StringUtil::split(configPath, "/");
    if(names.empty()) {
        return false;
    }
    string last = *names.rbegin();

    if (!StringUtil::strToInt32(last.c_str(), version)) {
        return false;
    }

    string temp = StringUtil::toString<int32_t>(version);
    
    if (temp == last) {
        return true;
    }
    
    return false;
}

bool HaPathDefs::getConfigRootPath(const string &configPath,
                                   string &rootPath)
{
    vector<string> names = StringUtil::split(configPath, "/", false);
    if(names.size() < (size_t)2) {
        return false;
    }
    while (!names.empty()) {
        string last = *(names.rbegin());
        names.pop_back();
        if (!last.empty()) {
            break;
        }
    }
    rootPath = "";
    for (vector<string>::const_iterator it = names.begin();
         it != names.end(); ++it)
    {
        rootPath = rootPath + *it + '/';
    }
    return true;
}

bool HaPathDefs::partitionIdToIndexPath(const PartitionID &partitionId,
            const string &indexWorkerDir,  string &indexPath)
{
    if (!partitionId.has_fullversion()
        || !partitionId.has_range()
        || !partitionId.has_clustername())
    {
        return false;
    }
    indexPath = constructIndexPath(FileUtil::joinFilePath(indexWorkerDir, "runtimedata"),
                                   partitionId.clustername(),
                                   partitionId.fullversion(), 
                                   partitionId.range());
    return true;
}

string HaPathDefs::constructIndexPath(const string &indexRoot, 
                                      const string &clusterName,
                                      uint32_t fullVersion, 
                                      const Range &range)
{
    return constructIndexPath(indexRoot, clusterName, fullVersion, range.from(), range.to());
}

string HaPathDefs::constructIndexPath(const string &indexRoot, 
                                      const string &clusterName,
                                      uint32_t fullVersion, 
                                      uint16_t from, uint16_t to)
{
    return constructIndexPath(FileUtil::normalizeDir(indexRoot) + 
                              clusterName + "/", fullVersion, from, to);
}

string HaPathDefs::constructIndexPath(const string &clusterRoot,
                                      uint32_t fullVersion,
                                      uint16_t from, uint16_t to)
{
    return FileUtil::normalizeDir(clusterRoot).append( 
            constructPartitionPrefix(fullVersion, from, to));
}

string HaPathDefs::constructPartitionPrefix(uint32_t fullVersion,
                                uint16_t from, uint16_t to)
{
    return VERSION_PREFIX
        + StringUtil::toString(fullVersion) + "/"
        + RANGE_PREFIX
        + StringUtil::toString(from)
        + PATH_NAME_SEPRATOR
        + StringUtil::toString(to) + "/";
}

string HaPathDefs::constructGenerationPath(const string &indexRoot, 
        const string &clusterName, uint32_t fullVersion)
{
    return FileUtil::normalizeDir(indexRoot)
        + clusterName + "/"
        + VERSION_PREFIX
        + StringUtil::toString(fullVersion) + "/";
}

string HaPathDefs::makeWorkerAddress(const string &ip, uint16_t port)
{
    return ip + ":" + StringUtil::toString(port);
}

bool HaPathDefs::parseWorkerAddress(const string &address,
                                    string &ip, uint16_t &port)
{
    const vector<string> &ads = StringUtil::split(address, ":");
    if(2 == ads.size()) {
        ip = ads[0];
        if (StringUtil::strToUInt16(ads[1].c_str(), port)) {
            return true;
        }
    }
    return false;
}

bool HaPathDefs::indexSourceToPartitionID(const string &indexSource,
        RoleType role, PartitionID &partitionId) 
{
    const vector<string> &names = StringUtil::split(indexSource, "/");
    if(names.size() < 3) {
        return false;
    }
    uint16_t from = 0;
    uint16_t to = 0;
    uint32_t version = 0;
    vector<string>::const_reverse_iterator it = names.rbegin();
    if (false == dirNameToRange(*it, from, to)) {
        HA3_LOG(WARN, "Dir name [%s] is invalid", (*it).c_str());
        return false;
    }
    ++it;
    if (false == dirNameToVersion(*it, version)) {
        return false;
    }
    ++it;
    if ((*it).empty()) {
        return false;
    }
    partitionId = ProtoClassUtil::createPartitionID(*it, role,
            from, to, version);
    return true;
}

bool HaPathDefs::dirNameToRange(const string &name,
                                uint16_t &from,
                                uint16_t &to)
{
    size_t pos = name.find(RANGE_PREFIX);
    if (0 != pos) {
        return false;
    }
    pos = name.find(PATH_NAME_SEPRATOR, RANGE_PREFIX.size());
    if (string::npos == pos) {
        return false;
    }
    
    string fromStr = name.substr(RANGE_PREFIX.size(),
            pos-RANGE_PREFIX.size());
    if (!StringUtil::strToUInt16(fromStr.c_str(), from)
        || fromStr != StringUtil::toString<uint16_t>(from)) 
    {
        return false;
    }

    string toStr = name.substr(pos+1);
    if (!StringUtil::strToUInt16(toStr.c_str(), to)
        || toStr != StringUtil::toString<uint16_t>(to) )
    {
        return false;
    }
    if (to < from) {
        return false;
    }
    return true;
}


bool HaPathDefs::dirNameToVersion(const string &name, uint32_t &version)
{
    size_t pos = name.find(VERSION_PREFIX);
    if (0 != pos) {
        return false;
    }

    string versionStr = name.substr(VERSION_PREFIX.size());
    if (!StringUtil::strToUInt32(versionStr.c_str(), version)
        || versionStr != StringUtil::toString<uint32_t>(version)) 
    {
        return false;
    }
    return true;
}

bool HaPathDefs::fileNameToVersion(const string &name, uint32_t &version)
{
    size_t pos = name.find(INCREMENTAL_VERSION_PREFIX);
    if (0 != pos) {
        return false;
    }

    string versionStr = name.substr(INCREMENTAL_VERSION_PREFIX.size());
    if (!StringUtil::strToUInt32(versionStr.c_str(), version)
        || versionStr != StringUtil::toString<uint32_t>(version)) 
    {
        return false;
    }
    return true;
}

bool HaPathDefs::dirNameToIncrementalVersion(const string &name,
            uint32_t &version)
{
    size_t pos = name.find(INCREMENTAL_VERSION_PREFIX);
    if (0 != pos) {
        return false;
    }

    string versionStr = name.substr(INCREMENTAL_VERSION_PREFIX.size());

    if (!StringUtil::strToUInt32(versionStr.c_str(), version)
        || versionStr != StringUtil::toString<uint32_t>(version)) 
    {
        return false;
    }
    return true;
}

bool HaPathDefs::generationIdCmp(uint32_t left, uint32_t right) {
    return left > right;
}

void HaPathDefs::sortGenerationList(FileList& fileList,
                                    vector<uint32_t>& generationIdVec) 
{
    for (FileListIterator it = fileList.begin();
         it != fileList.end();
         ++it)
    {
        size_t pos = (*it).find(VERSION_PREFIX);
        if (0 != pos) {
            continue;
        }
        
        uint32_t generationId = 0;
        string versionStr = (*it).substr(VERSION_PREFIX.size());
        if (!StringUtil::strToUInt32(versionStr.c_str(), generationId) || 
            (*it != VERSION_PREFIX + StringUtil::toString<uint32_t>(generationId)))
        {
            continue;
        }
        generationIdVec.push_back(generationId);
    }

    sort(generationIdVec.begin(), generationIdVec.end(), HaPathDefs::generationIdCmp);

    fileList.clear();
    for (vector<uint32_t>::const_iterator it = generationIdVec.begin();
         it != generationIdVec.end(); ++it)
    {
        fileList.push_back(VERSION_PREFIX + StringUtil::toString<uint32_t>(*it));
    }
}

END_HA3_NAMESPACE(common);

