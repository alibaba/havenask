#include "build_service/config/SchemaUpdatableClusterConfig.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, SchemaUpdatableClusterConfig);

SchemaUpdatableClusterConfig::SchemaUpdatableClusterConfig()
    : partitionCount(1)
{
}

SchemaUpdatableClusterConfig::~SchemaUpdatableClusterConfig() {
}

void SchemaUpdatableClusterConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("cluster_name", clusterName, clusterName);
    json.Jsonize("partition_count", partitionCount, partitionCount);
}

bool SchemaUpdatableClusterConfig::validate() const {
    if (clusterName.empty()) {
        string errorMsg = "cluster name can't be empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (partitionCount == 0) {
        string errorMsg = "partition count can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}


}
}
