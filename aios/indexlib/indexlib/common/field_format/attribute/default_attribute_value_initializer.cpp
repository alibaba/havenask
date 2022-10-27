#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DefaultAttributeValueInitializer);

DefaultAttributeValueInitializer::DefaultAttributeValueInitializer(
        const string& valueStr)
    : mValueStr(valueStr)
{
}

DefaultAttributeValueInitializer::~DefaultAttributeValueInitializer() 
{
}

bool DefaultAttributeValueInitializer::GetInitValue(
        docid_t docId, char* buffer, size_t bufLen) const
{
    assert(bufLen >= mValueStr.size());
    memcpy(buffer, mValueStr.c_str(), mValueStr.size());
    return true;
}

bool DefaultAttributeValueInitializer::GetInitValue(
        docid_t docId, ConstString& value, Pool *memPool) const
{
    value = GetDefaultValue(memPool);
    return true;
}

ConstString DefaultAttributeValueInitializer::GetDefaultValue(Pool *memPool) const
{
    return ConstString(mValueStr.c_str(), mValueStr.length(), memPool);
}

IE_NAMESPACE_END(common);

