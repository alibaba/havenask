#include <ha3/config/RankProfileConfig.h>

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, RankProfileConfig);

RankProfileConfig::RankProfileConfig() { 
    _rankProfileInfos.push_back(RankProfileInfo());
}

RankProfileConfig::~RankProfileConfig() { 
}

void RankProfileConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "modules", _modules);
    JSONIZE(json, "rankprofiles", _rankProfileInfos);
}

bool RankProfileConfig::operator==(const RankProfileConfig &other) const {
    if (&other == this) {
        return true;
    }
    if (_modules.size() != other._modules.size() ||
        _rankProfileInfos.size() != other._rankProfileInfos.size())
    {
        return false;
    }
    return _modules == other._modules
        && _rankProfileInfos == other._rankProfileInfos;
}

END_HA3_NAMESPACE(config);

