#ifndef __INDEXLIB_FAKE_ATTRIBUTE_MODIFIER_H
#define __INDEXLIB_FAKE_ATTRIBUTE_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class FakeAttributeModifier
{
public:
    FakeAttributeModifier(const config::AttributeSchemaPtr& attrSchema, 
                          util::pool* pool,
                          const std::vector<docid_t>& baseDocIdVect);

    ~FakeAttributeModifier() {}
public:
    
protected:
    virtual SegmentedAttributeModifierPtr CreateSegmentedModifier(segmentid_t segmentId);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeAttributeModifier);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_ATTRIBUTE_MODIFIER_H
