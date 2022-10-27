#ifndef INDEXLIB_ATOMICCOUNTER_H
#define INDEXLIB_ATOMICCOUNTER_H

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

#define INDEXLIB_LOCK "lock ; "

class AtomicCounter64
{
public:
    AtomicCounter64() {
        _counter = 0;
    }

    inline long getValue() const
    {
        return _counter;
    }

    inline void setValue(long c)
    {
        _counter = c;
    }

    inline void add(long i)
    {
        asm volatile(
            INDEXLIB_LOCK "addq %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
    }

    inline void sub(long i)
    {
        asm volatile(
            INDEXLIB_LOCK "subq %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
    }

    inline void inc() {
        asm volatile(
            INDEXLIB_LOCK "incq %0"
            :"=m" (_counter)
            :"m" (_counter));
    }

    inline void dec() {
        asm volatile(
            INDEXLIB_LOCK "decq %0"
            :"=m" (_counter)
            :"m" (_counter));
    }

private:
    volatile long _counter;
};

#undef INDEXLIB_LOCK

IE_NAMESPACE_END(util);

#endif //INDEXLIB_ATOMICCOUNTER_H


