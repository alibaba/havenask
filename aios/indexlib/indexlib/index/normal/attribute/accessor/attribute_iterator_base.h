#ifndef __INDEXLIB_ATTRIBUTE_ITERATOR_BASE_H
#define __INDEXLIB_ATTRIBUTE_ITERATOR_BASE_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <autil/mem_pool/Pool.h>
#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

class AttributeIteratorBase
{
public:
    AttributeIteratorBase(autil::mem_pool::Pool* pool);
    virtual ~AttributeIteratorBase();
public:
    /**
     * Reset iterator to begin state
     */
    virtual void Reset() = 0;
    
protected:
    autil::mem_pool::Pool* mPool;
};

typedef std::tr1::shared_ptr<AttributeIteratorBase> AttributeIteratorBasePtr;

IE_NAMESPACE_END(index);

#endif //__INDEXENGINEATTRIBUTE_ITERATOR_BASE_H
