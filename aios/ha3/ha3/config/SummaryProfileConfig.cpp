#include <ha3/config/SummaryProfileConfig.h>

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, SummaryProfileConfig);

SummaryProfileConfig::SummaryProfileConfig() { 
    _summaryProfileInfos.push_back(SummaryProfileInfo());
}

SummaryProfileConfig::~SummaryProfileConfig() { 
}

void SummaryProfileConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "modules", _modules);
    JSONIZE(json, "summary_profiles", _summaryProfileInfos);
    JSONIZE(json, "required_attribute_fields", _attributeFields);
}

END_HA3_NAMESPACE(config);

