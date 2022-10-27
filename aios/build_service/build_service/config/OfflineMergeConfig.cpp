#include "build_service/config/OfflineMergeConfig.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, OfflineMergeConfig);

OfflineMergeConfig::OfflineMergeConfig()
    : mergeParallelNum(0)
    , mergeSleepTime(0)
    , needWaitAlterField(true)
{
}

OfflineMergeConfig::~OfflineMergeConfig() {
}

void OfflineMergeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("merge_config", mergeConfig, mergeConfig);
    json.Jsonize("period", periodMergeDescription, periodMergeDescription);
    json.Jsonize("merge_parallel_num", mergeParallelNum, mergeParallelNum);
    json.Jsonize("merge_sleep_time", mergeSleepTime, mergeSleepTime);
    json.Jsonize("plugin_config", mergePluginConfig, mergePluginConfig);
    json.Jsonize("need_wait_alter_field", needWaitAlterField, needWaitAlterField);
}

}
}
