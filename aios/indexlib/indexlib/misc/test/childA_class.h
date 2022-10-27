#ifndef __INDEXLIB_CHILDCLASSA_CLASS_H
#define __INDEXLIB_CHILDCLASSA_CLASS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/test/base_class.h"

IE_NAMESPACE_BEGIN(misc);

/* 1. declare sub class type */
IE_SUB_CLASS_TYPE_DECLARE(BaseClass, ChildClassA);
class ChildClassA : public BaseClass
{
public:
    ChildClassA()
    {
        /* 2. setup sub class type in all constructor */
        IE_SUB_CLASS_TYPE_SETUP(BaseClass, ChildClassA);
    }
    
    ~ChildClassA() {}
    
public:
    void print() override final
    {
        std::cout << "i am A" << std::endl;
    }

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_CHILDCLASSA_CLASS_H
