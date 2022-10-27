#include <ha3/config/SearchOptimizerConfig.h>

using namespace std;

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, SearchOptimizerConfig);

SearchOptimizerConfig::SearchOptimizerConfig() {
}

SearchOptimizerConfig::~SearchOptimizerConfig() {
}

void SearchOptimizerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("modules", _modules, _modules);
    json.Jsonize("optimizers", _optimizerConfigInfos, _optimizerConfigInfos);
}

END_HA3_NAMESPACE(config);

