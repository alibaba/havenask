#ifndef __INDEXLIB_FLOAT_ATTRIBUTE_UPDATER_CREATOR_H
#define __INDEXLIB_FLOAT_ATTRIBUTE_UPDATER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_updater.h"

IE_NAMESPACE_BEGIN(index);

class FloatFp16AttributeUpdaterCreator : public AttributeUpdaterCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp16;
    }

    AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics,
                             segmentid_t segId, 
                             const config::AttributeConfigPtr& attrConfig) const
    {
        return new SingleValueAttributeUpdater<int16_t>(
                buildResourceMetrics, segId, attrConfig);

    }
};

class FloatInt8AttributeUpdaterCreator : public AttributeUpdaterCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp8;
    }
    
    AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics,
                             segmentid_t segId, 
                             const config::AttributeConfigPtr& attrConfig) const
    {
        return new SingleValueAttributeUpdater<int8_t>(
                buildResourceMetrics, segId, attrConfig);

    }
};

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_FLOAT_ATTRIBUTE_UPDATER_CREATOR_H
