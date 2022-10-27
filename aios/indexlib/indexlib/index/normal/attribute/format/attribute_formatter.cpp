#include "indexlib/index/normal/attribute/format/attribute_formatter.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeFormatter);

AttributeFormatter::AttributeFormatter() 
{
}

AttributeFormatter::~AttributeFormatter() 
{
}

void AttributeFormatter::SetAttrConvertor(
        const AttributeConvertorPtr& attrConvertor)
{
    assert (attrConvertor);
    mAttrConvertor = attrConvertor;
}

IE_NAMESPACE_END(index);

