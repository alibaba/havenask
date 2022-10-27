#ifndef __INDEXLIB_STRING_ATTRIBUTE_UPDATER_H
#define __INDEXLIB_STRING_ATTRIBUTE_UPDATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_updater.h"

IE_NAMESPACE_BEGIN(index);

class StringAttributeUpdater : public VarNumAttributeUpdater<char>
{
public:
    StringAttributeUpdater(util::BuildResourceMetrics* buildResourceMetrics,
                           segmentid_t segId,
                           const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeUpdater<char>(buildResourceMetrics, segId, attrConfig)
    {}
    ~StringAttributeUpdater() {}

public:
    class Creator : public AttributeUpdaterCreator
    {
    public:
        FieldType GetAttributeType() const
        {
            return ft_string;
        }

        AttributeUpdater* Create(util::BuildResourceMetrics* buildResourceMetrics,
                segmentid_t segId, const config::AttributeConfigPtr& attrConfig) const
        {
            return new StringAttributeUpdater(buildResourceMetrics, segId, attrConfig);
        }
    };
};

DEFINE_SHARED_PTR(StringAttributeUpdater);

///////////////////////////////////////////////////////

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_STRING_ATTRIBUTE_UPDATER_H
