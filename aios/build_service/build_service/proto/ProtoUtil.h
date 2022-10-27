#ifndef ISEARCH_BS_PROTOUTIL_H
#define ISEARCH_BS_PROTOUTIL_H

#include "build_service/common_define.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include <autil/legacy/jsonizable.h>
#include <beeper/beeper.h>
#include <sstream>

#define SET_ERROR_AND_RETURN(buildId, response, format, arg...)         \
    do {                                                                \
        char buffer[1024];                                              \
        snprintf(buffer, 1024, format, arg);                            \
        std::string errorMsg = std::string(buffer);                     \
        BS_LOG(WARN, "%s", errorMsg.c_str());                           \
        response->add_errormessage(buffer);                             \
        beeper::EventTags tags = BuildIdWrapper::getEventTags(buildId); \
        if (tags.Size() > 0) {                                          \
            BEEPER_REPORT(GENERATION_CMD_COLLECTOR_NAME, errorMsg, tags); \
        } else {                                                        \
            BEEPER_REPORT(ADMIN_STATUS_COLLECTOR_NAME, errorMsg, tags); \
        }                                                               \
        return;                                                         \
    } while(0)

namespace build_service {
namespace proto {

class BuildIdWrapper : public autil::legacy::Jsonizable
{
public:
    proto::BuildId buildId;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    static beeper::EventTags getEventTags(const proto::BuildId& buildId)
    {
        beeper::EventTags tags;
        if (buildId.has_generationid())
        {
            tags.AddTag("generation", autil::StringUtil::toString(buildId.generationid()));
        }
        return tags;
    }
};

class ProtoUtil
{
public:
    // roleId for processor: appName.dataTable.gid.processor.step.from.to.dataSourceId
    // roleId for builder: appName.dataTable.gid.builder.step.from.to.clusterName
    // roleId for merger: appName.dataTable.gid.merger.mergeConfigName.from.to.clusterName
    // roleId for job: appName.dataTable.gid.job.step.from.to.clusterName.dataSourceId
    static bool partitionIdToRoleId(const proto::PartitionId &pid,
                                    std::string &roleId);
    static bool roleIdToPartitionId(const std::string &roleId,
                                    const std::string &appName,
                                    proto::PartitionId &pid);

    static bool partitionIdToCounterFileName(const proto::PartitionId &pid,
            std::string &counterFileName);

    static std::string adminToRoleId(
            uint32_t generationId,
            const std::string &datatable = "",
            const std::string &clusterName = "");

    // swift topic related
    // prefix is applicationId 
    static std::string getRawDocTopicName(
            const std::string &prefix, const BuildId &buildId);
    static std::string getProcessedDocTopicName(
            const std::string &prefix, const BuildId &buildId,
            const std::string &clusterName);

    // Role related
    static std::string& toRoleString(RoleType role);
    static RoleType fromRoleString(const std::string &roleStr);

    static std::string toStepString(const PartitionId &pid);
    static std::string toStepString(const proto::BuildStep& buildStep);

    static std::vector<std::string> getClusterNames(const proto::PartitionId &partitionId);

    template<typename MessageType>
    static bool checkHeartbeat(const MessageType &message,
                               const PartitionId &pid,
                               bool exitOnMismatch = false);

    static bool isEqual(const BuildId& left, const BuildId& right);
    // BuildId related
    static bool strToBuildId(const std::string& str, proto::BuildId& buildId);
    static std::string buildIdToStr(const proto::BuildId& buildId);

    static void partitionIdToBeeperTags(const proto::PartitionId& pid,
            beeper::EventTags &tags);
    static void buildIdToBeeperTags(const proto::BuildId& buildId,
            beeper::EventTags &tags);
    
    static const char* getServiceErrorCodeName(ServiceErrorCode errorCode);
    static const char* getErrorAdviceName(ErrorAdvice errorAdvice);
    
private:
    static uint32_t getProcessorSchemaUpdateCount(const std::string& processorTaskId);
public:
    static std::string ROLE_NAME_BUILDER;
    static std::string ROLE_NAME_PROCESSOR;
    static std::string ROLE_NAME_MERGER;
    static std::string ROLE_NAME_TASK;
    static std::string ROLE_NAME_JOB;
    static std::string ROLE_NAME_UNKNOWN;
private:
    ProtoUtil();
    ~ProtoUtil();
private:
    BS_LOG_DECLARE();
};

template<typename MessageType>
bool ProtoUtil::checkHeartbeat(const MessageType &message,
                               const PartitionId &pid,
                               bool exitOnMismatch)
{
    if (message.has_partitionid() && message.partitionid() != pid) {
        std::stringstream ss;
        ss << "request pid[" << message.partitionid().ShortDebugString()
           << "] local pid ["<< pid.ShortDebugString() << "] not match";
        BS_LOG(WARN, "%s", ss.str().c_str());
        if (exitOnMismatch) {
            std::cerr << ss.str() << std::endl;
            std::string msg = "worker checkHeartbeat fail," + ss.str();            
            BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, msg);
            BEEPER_CLOSE();
            _exit(-1);
        }
        return false;
    } else if (message.has_roleid()) {
        std::string roleId;
        if (!ProtoUtil::partitionIdToRoleId(pid, roleId)) {
            BS_LOG(WARN, "pid[%s] to roleid failed", pid.ShortDebugString().c_str());
            return false;
        }
        if (message.roleid() != roleId) {
            std::stringstream ss;
            ss << "request role[" << message.roleid()
               << "] local role[" << roleId << "] not match";
            BS_LOG(WARN, "%s", ss.str().c_str());
            if (exitOnMismatch) {
                std::cerr << ss.str() << std::endl;
                std::string msg = "worker checkHeartbeat fail," + ss.str();
                BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, msg);
                BEEPER_CLOSE();
                _exit(-1);
            }
            return false;
        }
    } else {
        BS_LOG(WARN, "message[%s] has no roleid or pid", message.ShortDebugString().c_str());
        return false;
    }
    return true;
}

}
}

#endif //ISEARCH_BS_PROTOUTIL_H
