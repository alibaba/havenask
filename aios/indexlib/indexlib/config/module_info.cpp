#include "indexlib/config/module_info.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(config);

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

IE_NAMESPACE_END(config);

