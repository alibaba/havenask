#ifndef __INDEXLIB_CHILDB_CLASS_H
#define __INDEXLIB_CHILDB_CLASS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/test/base_class.h"

IE_NAMESPACE_BEGIN(misc);

IE_SUB_CLASS_TYPE_DECLARE(BaseClass, ChildClassB);
class ChildClassB : public BaseClass 
{
public:
    ChildClassB()
    {
        IE_SUB_CLASS_TYPE_SETUP(BaseClass, ChildClassB);
    }
    
    ~ChildClassB() {}
    
public:
    void print() override final
    {
        std::cout << "i am B" << std::endl;
    }

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_DERIVE_CLASS_H
