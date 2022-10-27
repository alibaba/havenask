#include "indexlib/util/metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

Metrics::~Metrics() 
{
}

void Metrics::Reset() 
{
    if (mParent)
    {
        mParent->SubValue(mValue);
    }

    asm volatile(
            "lock ; "  "xor %1,%0"
            :"=m" (mValue)
            :"ir" (mValue), "m" (mValue));


    asm volatile(
            "lock ; "  "xor %1,%0"
            :"=m" (mCount)
            :"ir" (mCount), "m" (mCount));
}

IE_NAMESPACE_END(util);

