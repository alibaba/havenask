#ifndef ISEARCH_BS_PROTOCREATOR_H
#define ISEARCH_BS_PROTOCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service {
namespace proto {

class ProtoCreator
{
public:
    ProtoCreator();
    ~ProtoCreator();
private:
    ProtoCreator(const ProtoCreator &);
    ProtoCreator& operator=(const ProtoCreator &);
public:
    static BuildId createBuildId(const std::string &dataTable = "",
                                 generationid_t generationId = -1,
                                 const std::string &appName = "");
    static PartitionId createPartitionId(
            RoleType role, BuildStep step = BUILD_STEP_FULL,
            uint16_t from = 0, uint16_t to = 65535,
            const std::string &dataTable = "",
            generationid_t generationId = -1,
            const std::string &clusterNames = "simple",
            const std::string &mergeConfigName = "",
            const std::string &taskid = "",
            const std::string &appName = "");

    static std::vector<PartitionId> createPartitionIds(
            uint32_t partCount, uint32_t parallelNum, RoleType role,
            BuildStep step = BUILD_STEP_FULL, 
            uint16_t from = 0, uint16_t to = 65535,
            const std::string &dataTable = "",
            generationid_t gid = 0,
            const std::string &clusterNames = "simple",
            const std::string &mergeConfigName = "");
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_PROTOCREATOR_H
