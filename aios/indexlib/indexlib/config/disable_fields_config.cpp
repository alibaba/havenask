#include "indexlib/config/disable_fields_config.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DisableFieldsConfig);

DisableFieldsConfig::DisableFieldsConfig() 
{
}

DisableFieldsConfig::~DisableFieldsConfig() 
{
}

void DisableFieldsConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("attributes", attributes, attributes);
    json.Jsonize("indexes", indexes, indexes);
    json.Jsonize("pack_attributes", packAttributes, packAttributes);
    json.Jsonize("rewrite_load_config", rewriteLoadConfig, rewriteLoadConfig);
}

bool DisableFieldsConfig::operator == (const DisableFieldsConfig& other) const
{
    return attributes == other.attributes && packAttributes == other.packAttributes 
        && indexes == other.indexes;
}

IE_NAMESPACE_END(config);

