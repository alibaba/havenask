#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeDataIterator);

AttributeDataIterator::AttributeDataIterator(const AttributeConfigPtr& attrConfig)
    : mAttrConfig(attrConfig)
    , mDocCount(0)
    , mCurDocId(0)
{
    assert(mAttrConfig);
}

AttributeDataIterator::~AttributeDataIterator() 
{
}

IE_NAMESPACE_END(index);

