#ifndef __INDEXLIB_EXECUTOR_H
#define __INDEXLIB_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

class Executor
{
public:
    Executor() {}
    virtual ~Executor() {};
public:
    virtual void Execute() = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Executor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EXECUTOR_H
