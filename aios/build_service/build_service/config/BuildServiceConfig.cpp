#include "build_service/config/BuildServiceConfig.h"

using namespace std;
namespace build_service {
namespace config {
BS_LOG_SETUP(config, BuildServiceConfig);


BuildServiceConfig::BuildServiceConfig()
    : amonitorPort(DEFAULT_AMONITOR_PORT)
{
}

BuildServiceConfig::~BuildServiceConfig() {
}
    
BuildServiceConfig::BuildServiceConfig(const BuildServiceConfig& other)
    : amonitorPort(other.amonitorPort)
    , userName(other.userName)
    , serviceName(other.serviceName)
    , swiftRoot(other.swiftRoot)
    , hippoRoot(other.hippoRoot)
    , zkRoot(other.zkRoot)
    , heartbeatType(other.heartbeatType)
    , counterConfig(other.counterConfig)
    , _fullBuilderTmpRoot(other._fullBuilderTmpRoot)
    , _indexRoot(other._indexRoot)
{
}
    
void BuildServiceConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("user_name", userName, userName);
    json.Jsonize("service_name", serviceName, serviceName);
    json.Jsonize("index_root", _indexRoot, _indexRoot);
    json.Jsonize("swift_root", swiftRoot, swiftRoot);
    json.Jsonize("hippo_root", hippoRoot, hippoRoot);
    json.Jsonize("zookeeper_root", zkRoot, zkRoot);
    json.Jsonize("amonitor_port", amonitorPort, amonitorPort);
    json.Jsonize("heartbeat_type", heartbeatType, heartbeatType);
    json.Jsonize("counter_config", counterConfig, counterConfig);
    json.Jsonize("full_builder_tmp_root", _fullBuilderTmpRoot, _fullBuilderTmpRoot);
}

bool BuildServiceConfig::operator == (const BuildServiceConfig &other) const {
    return (userName == other.userName &&
            serviceName == other.serviceName &&
            _indexRoot == other._indexRoot &&
            swiftRoot == other.swiftRoot &&
            hippoRoot == other.hippoRoot &&
            zkRoot == other.zkRoot &&
            amonitorPort == other.amonitorPort &&
            heartbeatType == other.heartbeatType &&
            counterConfig == other.counterConfig &&
            _fullBuilderTmpRoot == other._fullBuilderTmpRoot);
} 

string BuildServiceConfig::getIndexRoot() const {
    return _indexRoot;
}
    
string BuildServiceConfig::getBuilderIndexRoot(bool isFullBuilder) const {
    if (_fullBuilderTmpRoot.empty()){
        return _indexRoot;
    }
    return isFullBuilder ? _fullBuilderTmpRoot : _indexRoot;
}

string BuildServiceConfig::getApplicationId() const {
    return userName + "_" + serviceName;
}

string BuildServiceConfig::getMessageIndexName() const {
    return userName + "_" + serviceName;
}

bool BuildServiceConfig::validate() const {
    if (userName.empty()) {
        string errorMsg = "UserName can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (serviceName.empty()) {
        string errorMsg = "ServiceName can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_indexRoot.empty()) {
        string errorMsg = "IndexRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (swiftRoot.empty()) {
        string errorMsg = "SwiftRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (hippoRoot.empty()) {
        string errorMsg = "HippoRoot can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (zkRoot.empty()) {
        string errorMsg = "Zookeeper can't be empty.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!counterConfig.validate()) {
        BS_LOG(ERROR, "%s", "invalid counter_config");
        return false;
    }
    return true;
}

}
}
