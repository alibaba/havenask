#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/config/field_config.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, VirtualAttributeConfigCreator);

AttributeConfigPtr VirtualAttributeConfigCreator::Create(
        const string &name, FieldType type,
        bool multiValue, const string &defaultValue,
        const common::AttributeValueInitializerCreatorPtr& creator)
{
    FieldConfigPtr fieldConfig(new FieldConfig(name, type, multiValue, true, false));
    fieldConfig->SetDefaultValue(defaultValue);
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(fieldConfig, creator);
    return attrConfig;
}

IE_NAMESPACE_END(config);

