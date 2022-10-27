#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeWriter);

void AttributeWriter::SetAttrConvertor(const AttributeConvertorPtr& attrConvertor)
{
    assert (attrConvertor);
    mAttrConvertor = attrConvertor;
}

IE_NAMESPACE_END(index);

