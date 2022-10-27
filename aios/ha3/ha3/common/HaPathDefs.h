#ifndef ISEARCH_HAPATHDEFS_H
#define ISEARCH_HAPATHDEFS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <fslib/fslib.h>

BEGIN_HA3_NAMESPACE(common);

class HaPathDefs
{
public:
    static bool configPathToVersion(const std::string &configPath,
                                    int32_t &version);
    static bool getConfigRootPath(const std::string &configPath,
                                  std::string &rootPath);
    // online path
    static bool partitionIdToIndexPath(const proto::PartitionID &partitionId,
            const std::string &indexWorkerDir,  std::string &indexPath);
    static bool indexSourceToPartitionID(const std::string &indexSource,
            proto::RoleType role, proto::PartitionID &partitionId);

    static std::string constructIndexPath(const std::string &indexRoot, 
            const std::string &clusterName, uint32_t fullVersion, 
            const proto::Range &range);

    static std::string constructGenerationPath(const std::string &indexRoot, 
            const std::string &clusterName, uint32_t fullVersion);

    static std::string constructIndexPath(const std::string &indexRoot, 
            const std::string &clusterName, uint32_t fullVersion, 
            uint16_t from, uint16_t to);

    static std::string constructIndexPath(const std::string &clusterRoot,
            uint32_t fullVersion, uint16_t from, uint16_t to);

    static std::string constructPartitionPrefix(uint32_t fullVersion,
            uint16_t from, uint16_t to);
    
    static std::string makeWorkerAddress(const std::string &ip,
            uint16_t port);
    static bool parseWorkerAddress(const std::string &address,
                                   std::string &ip, uint16_t &port);
    static bool dirNameToRange(const std::string &name,
                               uint16_t &from,
                               uint16_t &to);
    static bool dirNameToVersion(const std::string &name,
                                 uint32_t &version);
    static bool fileNameToVersion(const std::string &name,
                                  uint32_t &version);
    static bool dirNameToIncrementalVersion(const std::string &name,
            uint32_t &version);

    static bool generationIdCmp(uint32_t left, uint32_t right);
    
    static void sortGenerationList(fslib::FileList& fileList,
                                   std::vector<uint32_t>& generationIdVec);

private:
    HA3_LOG_DECLARE();
};


END_HA3_NAMESPACE(common);

#endif //ISEARCH_HAPATHDEFS_H
