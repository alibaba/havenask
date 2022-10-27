#ifndef ISEARCH_BS_JOBRANGEUTIL_H
#define ISEARCH_BS_JOBRANGEUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/config/BuildRuleConfig.h"

namespace build_service {
namespace task_base {

class JobConfig
{
public:
    JobConfig();
    ~JobConfig();
public:
    bool init(const KeyValueMap &kvMap);
public:
    proto::Range calculateBuildMapRange(uint32_t instanceId) const;
    proto::Range calculateBuildReduceRange(uint32_t instanceId) const;
    proto::Range calculateMergeMapRange(uint32_t instanceId) const;
    proto::Range calculateMergeReduceRange(uint32_t instanceId) const;
    std::vector<proto::Range> splitBuildParts() const;
    std::vector<proto::Range> splitFinalParts() const;
    std::vector<proto::Range> getAllNeedMergePart(const proto::Range &mergeRange) const;

    std::vector<int32_t> calcMapToReduceTable() const;

    bool needPartition() const {
        return _config.needPartition;
    }
    const std::string &getClusterName() const {
        return _clusterName;
    }
    generationid_t getGenerationId() const {
        return _generationId;
    }
    const std::string &getBuildMode() const {
        return _buildMode;
    }
    uint16_t getAmonitorPort() const {
        return _amonitorPort;
    }
    const config::BuildRuleConfig &getBuildRuleConf() const {
        return _config;
    }
    void setBuildRuleConfig(const config::BuildRuleConfig &config) {
        _config = config;
    }
private:
    void initBuildRuleConfig(const KeyValueMap &kvMap);
    std::vector<proto::Range> splitMergeParts();
private:
    template <typename T>
    static T getValueWithDefault(const KeyValueMap &kvMap,
                                 const std::string &keyName,
                                 const T &defaultValue = T());
    static std::vector<int32_t> calcMapToReduceTable(
            int32_t buildPartFrom, int32_t buildPartCount,
            int32_t partitionCount, int32_t buildParallelNum);
private:
    std::string _clusterName;
    generationid_t _generationId;
    uint32_t _buildPartitionFrom;
    uint32_t _buildPartitionCount;
    config::BuildRuleConfig _config;
    std::string _buildMode;
    uint16_t _amonitorPort;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(JobConfig);

}
}

#endif //ISEARCH_BS_JOBRANGEUTIL_H
