#include "build_service/plugin/ModuleInfo.h"

using namespace std;
namespace build_service {
namespace plugin {

void ModuleInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("module_name", moduleName);
    json.Jsonize("module_path", modulePath);
    json.Jsonize("parameters", parameters, KeyValueMap());
}

bool ModuleInfo::operator==(const ModuleInfo &other) const {
    if (&other == this) {
        return true;
    }
    return moduleName == other.moduleName
        && modulePath == other.modulePath
        && parameters == other.parameters;
}

}
}
