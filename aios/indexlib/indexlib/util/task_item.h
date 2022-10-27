#ifndef __INDEXLIB_TASK_ITEM_H
#define __INDEXLIB_TASK_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class TaskItem
{
public:
    TaskItem() {}
    virtual ~TaskItem() {}
public:
    virtual void Run() = 0;
};

DEFINE_SHARED_PTR(TaskItem);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TASK_ITEM_H
