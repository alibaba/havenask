#ifndef __INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H
#define __INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/task_item.h"

IE_NAMESPACE_BEGIN(util);

template <class X, void (X::*p)()>
class MemberFunctionTaskItem : public TaskItem
{
public:
    MemberFunctionTaskItem(X& ref)
        : mRef(ref) {}
    ~MemberFunctionTaskItem() {}
public:
    void Run() override
    {
        (mRef.*p)();
    }
private:
    X& mRef;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H
