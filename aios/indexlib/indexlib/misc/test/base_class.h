#ifndef __INDEXLIB_BASE_CLASS_H
#define __INDEXLIB_BASE_CLASS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/devirtual.h"

IE_NAMESPACE_BEGIN(misc);

/* 1. declare all sub class */
IE_BASE_DECLARE_SUB_CLASS_BEGIN(BaseClass)
IE_BASE_DECLARE_SUB_CLASS(BaseClass, ChildClassA)
IE_BASE_DECLARE_SUB_CLASS(BaseClass, ChildClassB)
IE_BASE_DECLARE_SUB_CLASS_END(BaseClass)

class BaseClass
{
public:
    BaseClass();
    virtual ~BaseClass();

    /* 2. declare base class */
    IE_BASE_CLASS_DECLARE(BaseClass);
    
public:
    virtual void print() = 0;

public:
    /* 3. add impl function to replace print */ 
    inline void printImpl() __ALWAYS_INLINE;
};

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_BASE_CLASS_H
