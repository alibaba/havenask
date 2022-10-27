#ifndef __INDEXLIB_NUMERIC_UTIL_H
#define __INDEXLIB_NUMERIC_UTIL_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

class NumericUtil
{
public:
    static uint32_t UpperPack(uint32_t number, uint32_t pack)
    {
        return ((number + pack - 1) / pack) * pack;
    }
};

typedef std::tr1::shared_ptr<NumericUtil> NumericUtilPtr;

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_NUMERIC_UTIL_H
