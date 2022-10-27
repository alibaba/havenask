#include "build_service/config/OfflineIndexConfigMap.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, OfflineIndexConfigMap);

OfflineIndexConfigMap::OfflineIndexConfigMap() {
}

OfflineIndexConfigMap::~OfflineIndexConfigMap() {
}

void OfflineIndexConfigMap::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    OfflineIndexConfig& defaultOfflineConfig = _offlineIndexConfigMap[""];
    json.Jsonize("build_config",
                 defaultOfflineConfig.buildConfig,
                 defaultOfflineConfig.buildConfig);
    json.Jsonize("merge_config",
                 defaultOfflineConfig.offlineMergeConfig.mergeConfig,
                 defaultOfflineConfig.offlineMergeConfig.mergeConfig);
    typedef map<string, OfflineMergeConfig> MergeConfigMap;
    MergeConfigMap mergeConfigMap;

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("customized_merge_config", mergeConfigMap, mergeConfigMap);
        MergeConfigMap::iterator it = mergeConfigMap.begin();
        for (; it != mergeConfigMap.end(); ++it) {
            OfflineIndexConfig offlineIndexConfig = defaultOfflineConfig;
            offlineIndexConfig.offlineMergeConfig = it->second;
            _offlineIndexConfigMap[it->first] = offlineIndexConfig;
        }
    }
    else {
        InnerMap::iterator it = _offlineIndexConfigMap.begin();
        for (; it != _offlineIndexConfigMap.end(); ++it) {
            if (it->first != "") {
                mergeConfigMap[it->first] = it->second.offlineMergeConfig;
            }
        }
        json.Jsonize("customized_merge_config", mergeConfigMap);
    }
}
}
}
