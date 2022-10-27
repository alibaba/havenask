#ifndef __INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/pack_attribute_config.h"

IE_NAMESPACE_BEGIN(common);

class AttributeConvertorFactory : public misc::Singleton<AttributeConvertorFactory>
{
public:
    ~AttributeConvertorFactory();
public:
    AttributeConvertor* CreateAttrConvertor(
            const config::FieldConfigPtr& fieldConfig, TableType tableType);
    
    AttributeConvertor* CreatePackAttrConvertor(
            const config::PackAttributeConfigPtr& packAttrConfig);

public:
    AttributeConvertor* CreateAttrConvertor(
            const config::FieldConfigPtr& fieldConfig);

private:
    AttributeConvertor* CreateSingleAttrConvertor(const config::FieldConfigPtr& fieldConfig);
    AttributeConvertor* CreateMultiAttrConvertor(const config::FieldConfigPtr& fieldConfig);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeConvertorFactory);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_CONVERTOR_FACTORY_H
