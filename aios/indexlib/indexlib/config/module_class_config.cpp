#include "indexlib/config/module_class_config.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, ModuleClassConfig);

ModuleClassConfig::ModuleClassConfig()
{
}

ModuleClassConfig::~ModuleClassConfig() 
{
}

void ModuleClassConfig::Jsonize(Jsonizable::JsonWrapper& json) {
    json.Jsonize("class_name", className, className);
    json.Jsonize("module_name", moduleName, moduleName);
    json.Jsonize("parameters", parameters, parameters);    
}

bool ModuleClassConfig::operator==(const ModuleClassConfig& other) const
{
    return className == other.className && moduleName == other.moduleName
        && parameters == other.parameters;
}

ModuleClassConfig& ModuleClassConfig::operator=(
    const ModuleClassConfig& other)
{
    className = other.className;
    moduleName = other.moduleName;
    parameters = other.parameters;
    return *this;
}

IE_NAMESPACE_END(config);

