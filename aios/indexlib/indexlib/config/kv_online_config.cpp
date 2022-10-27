#include "indexlib/config/kv_online_config.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KVOnlineConfig);

KVOnlineConfig::KVOnlineConfig() 
{
}

KVOnlineConfig::~KVOnlineConfig() 
{
}

void KVOnlineConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    // for kkv
    json.Jsonize("count_limits", countLimits, countLimits);
    json.Jsonize("build_protection_threshold", buildProtectThreshold,
                 buildProtectThreshold);
}

IE_NAMESPACE_END(config);

