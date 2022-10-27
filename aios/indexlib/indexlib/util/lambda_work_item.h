#ifndef __INDEXLIB_LAMBDA_WORK_ITEM_H
#define __INDEXLIB_LAMBDA_WORK_ITEM_H

#include <tr1/memory>
#include "autil/WorkItem.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

template<typename Func>
class LambdaWorkItem : public autil::WorkItem
{
public:
    LambdaWorkItem(Func& func) : mFunc(func) {}
    ~LambdaWorkItem() {}
    void process() override { mFunc(); }
    void destroy() override { delete this; }
    void drop() override { destroy(); }
private:
    Func mFunc;
};

template<typename Func>
LambdaWorkItem<Func>* makeLambdaWorkItem(Func&& func)
{
    return new LambdaWorkItem<Func>(func);
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_LAMBDA_WORK_ITEM_H
