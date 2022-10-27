#ifndef __INDEXLIB_PRIME_NUMBER_TABLE_H
#define __INDEXLIB_PRIME_NUMBER_TABLE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class PrimeNumberTable
{
public:
    PrimeNumberTable();
    ~PrimeNumberTable();

public:
    static int64_t FindPrimeNumber(int64_t num);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimeNumberTable);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PRIME_NUMBER_TABLE_H
