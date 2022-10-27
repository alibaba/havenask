#ifndef __INDEXLIB_VIRTUALATTRIBUTECONFIGCREATOR_H
#define __INDEXLIB_VIRTUALATTRIBUTECONFIGCREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"

DECLARE_REFERENCE_CLASS(common, AttributeValueInitializerCreator);

IE_NAMESPACE_BEGIN(config);

class VirtualAttributeConfigCreator
{
private:
    VirtualAttributeConfigCreator();
    ~VirtualAttributeConfigCreator();
public:
    static AttributeConfigPtr Create(
            const std::string &name, FieldType type,
            bool multiValue, const std::string &defaultValue,
            const common::AttributeValueInitializerCreatorPtr& creator
            = common::AttributeValueInitializerCreatorPtr());
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VirtualAttributeConfigCreator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_VIRTUALATTRIBUTECONFIGCREATOR_H
