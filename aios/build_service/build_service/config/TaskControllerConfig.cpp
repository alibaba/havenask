#include "build_service/config/TaskControllerConfig.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, TaskControllerConfig);

TaskControllerConfig::TaskControllerConfig()
    : _type(CT_BUILDIN)
{
}

TaskControllerConfig::~TaskControllerConfig() {
}

void TaskControllerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    if (json.GetMode() == FROM_JSON) {
        string type;
        json.Jsonize("type", type);
        _type = strToType(type);
    } else {
        string typeStr = typeToStr();
        json.Jsonize("type", typeStr);
    }
    json.Jsonize("target_configs", _targets);
}

std::string TaskControllerConfig::typeToStr() {
    if (_type == CT_BUILDIN) {
        return "BUILDIN";
    }
    if (_type == CT_CUSTOM) {
        return "CUSTOM";
    }
    return "UNKOWN";
}

TaskControllerConfig::Type TaskControllerConfig::strToType(
    const std::string& str) {
    if (str == "BUILDIN") {
        return CT_BUILDIN;
    }

    if (str == "CUSTOM") {
        return CT_CUSTOM;
    }
    return CT_UNKOWN;
}
    
}
}
