#include "build_service/config/ProcessorRuleConfig.h"

using namespace std;

namespace build_service {
namespace config {

BS_LOG_SETUP(config, ProcessorRuleConfig);

ProcessorRuleConfig::ProcessorRuleConfig()
    : partitionCount(1)
    , parallelNum(1)
{
}

ProcessorRuleConfig::~ProcessorRuleConfig() {

}

void ProcessorRuleConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("partition_count", partitionCount, partitionCount);
    json.Jsonize("parallel_num", parallelNum, parallelNum);
}

bool ProcessorRuleConfig::validate() const {
    if (partitionCount == 0 || parallelNum == 0) {
        string errorMsg = "partition and parallelNum count can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

}
}
