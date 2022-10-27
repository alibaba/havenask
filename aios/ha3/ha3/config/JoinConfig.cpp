#include <ha3/config/JoinConfig.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(config);

HA3_LOG_SETUP(config, JoinConfig);

JoinConfig::JoinConfig() {
}

JoinConfig::~JoinConfig() {
}

void JoinConfig::Jsonize(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    JSONIZE(json, "scan_join_cluster", _scanJoinCluster);
    JSONIZE(json, "join_infos", _joinConfigInfos);
    doCompatibleWithOldFormat(json);
}

void JoinConfig::doCompatibleWithOldFormat(autil::legacy::Jsonizable::JsonWrapper& json) {
    bool use_join_cache = false;
    JSONIZE(json, "use_join_cache", use_join_cache);
    if (use_join_cache) {
        for (size_t i = 0; i < _joinConfigInfos.size(); ++i) {
            _joinConfigInfos[i].useJoinCache = true;
        }
    }
}

void JoinConfig::getJoinClusters(vector<string> &joinClusters) const {
    vector<JoinConfigInfo>::const_iterator it;
    for (it = _joinConfigInfos.begin(); 
         it != _joinConfigInfos.end(); it++) 
    {
        joinClusters.push_back((*it).getJoinCluster());
    }
}

void JoinConfig::getJoinFields(vector<string> &joinFields) const {
    vector<JoinConfigInfo>::const_iterator it;
    for (it = _joinConfigInfos.begin(); 
         it != _joinConfigInfos.end(); it++) 
    {
        joinFields.push_back((*it).getJoinField());
    }    
}

END_HA3_NAMESPACE(config);

