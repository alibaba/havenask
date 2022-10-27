#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"

using namespace autil::mem_pool;
IE_NAMESPACE_BEGIN(index);

AttributeIteratorBase::AttributeIteratorBase(Pool* pool)
    : mPool(pool)
{
}

AttributeIteratorBase::~AttributeIteratorBase() 
{
}

IE_NAMESPACE_END(index);

