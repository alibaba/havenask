#ifndef __INDEXLIB_MATH_UTIL_H
#define __INDEXLIB_MATH_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class MathUtil
{
public:
    MathUtil();
    ~MathUtil();
public:
    static size_t MultiplyByPercentage(size_t num, size_t percent)
    {
        return (num * percent) / 100;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MathUtil);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MATH_UTIL_H
