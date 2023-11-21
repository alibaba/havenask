#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class FakeAttributeModifier
{
public:
    FakeAttributeModifier(const config::AttributeSchemaPtr& attrSchema, util::pool* pool,
                          const std::vector<docid_t>& baseDocIdVect);

    ~FakeAttributeModifier() {}

public:
protected:
    virtual SegmentedAttributeModifierPtr CreateSegmentedModifier(segmentid_t segmentId);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeAttributeModifier);
}} // namespace indexlib::index
