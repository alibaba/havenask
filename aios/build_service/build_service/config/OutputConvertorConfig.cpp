#include "build_service/config/OutputConvertorConfig.h"

using namespace std;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, OutputConvertorConfig);

OutputConvertorConfig::OutputConvertorConfig() {
}

OutputConvertorConfig::~OutputConvertorConfig() {
}

void OutputConvertorConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("module", moduleName, moduleName);
    json.Jsonize("name", name, name);
    json.Jsonize("parameters", parameters, parameters);        
}
}
}
