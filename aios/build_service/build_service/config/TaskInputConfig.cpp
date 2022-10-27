#include "build_service/config/TaskInputConfig.h"

using namespace std;
using namespace autil::legacy;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, TaskInputConfig);

TaskInputConfig::TaskInputConfig()
{
}

TaskInputConfig::~TaskInputConfig() {
}

void TaskInputConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("type", _type, _type);
    json.Jsonize("module_name", _moduleName, _moduleName);    
    json.Jsonize("params", _parameters, KeyValueMap());
}

// std::string TaskInputConfig::typeToStr() {
//     if (_type == IT_INDEXLIB_INDEX) {
//         return "INDEXLIB_INDEX";
//     }
//     if (_type == IT_DB_MYSQL) {
//         return "DB_MYSQL";
//     }
//     if (_type == IT_UNKNOWN) {
//         return "UNKNOWN";
//     }
//     return "UNKOWN";
// }

// TaskInputConfig::InputType TaskInputConfig::strToType(const std::string& str) {
//     if (str == "UNKNOWN") {
//         return IT_UNKNOWN;
//     }

//     if (str == "INDEXLIB_INDEX") {
//         return IT_INDEXLIB_INDEX;
//     }

//     if (str == "DB_MYSQL") {
//         return IT_DB_MYSQL;
//     }
//     return IT_UNKNOWN;
// }
    
}
}
