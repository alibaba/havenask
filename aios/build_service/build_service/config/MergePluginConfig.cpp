#include "build_service/config/MergePluginConfig.h"
#include "build_service/config/ConfigDefine.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, MergePluginConfig);

MergePluginConfig::MergePluginConfig() 
    : _processorInfo(DEFAULT_ALTER_FIELD_MERGER_NAME)
{
}

MergePluginConfig::~MergePluginConfig() {
}

void MergePluginConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("module_info", _moduleInfo, _moduleInfo);
    json.Jsonize("class_info", _processorInfo, _processorInfo);
}

}
}
