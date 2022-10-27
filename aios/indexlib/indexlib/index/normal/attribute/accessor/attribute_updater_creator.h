#ifndef __INDEXLIB_ATTRIBUTE_UPDATER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_UPDATER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/mem_pool/Pool.h>
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"

IE_NAMESPACE_BEGIN(index);

class AttributeUpdaterCreator
{
public:
    AttributeUpdaterCreator(){}
    virtual ~AttributeUpdaterCreator() {}

public:
    virtual FieldType GetAttributeType() const = 0;
    virtual AttributeUpdater* Create(
            util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId, 
            const config::AttributeConfigPtr& attrConfig) const = 0;
};

typedef std::tr1::shared_ptr<AttributeUpdaterCreator> AttributeUpdaterCreatorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_UPDATER_CREATOR_H
