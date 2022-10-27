#ifndef __INDEXLIB_PRIMARY_KEY_HASH_CONVERTOR_H
#define __INDEXLIB_PRIMARY_KEY_HASH_CONVERTOR_H

#include <tr1/memory>
#include <autil/LongHashValue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyHashConvertor
{
public:
    PrimaryKeyHashConvertor() {}
    ~PrimaryKeyHashConvertor() {}
public:
    static void ToUInt128(uint64_t in, autil::uint128_t &out)
    {
        out.value[1] = in;
    }

    static void ToUInt64(const autil::uint128_t &in, uint64_t &out)
    {
        out = in.value[1];
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyHashConvertor);



IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_HASH_CONVERTOR_H
