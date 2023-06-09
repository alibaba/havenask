/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once


namespace autil {

#define AUTIL_LOCK "lock ; "

#if __aarch64__

//operations used in 32bit atomic counter in aarch64 architecture 

#define AUTIL_ATOMIC_OP(op, asm_op)                                  \
    unsigned long tmp;                                               \
    int result;                                                      \
                                                                     \
    asm volatile("// atomic_" #op "\n"                               \
"   prfm    pstl1strm, %2\n"                                         \
"1: ldxr    %w0, %2\n"                                               \
"   " #asm_op " %w0, %w0, %w3\n"                                     \
"   stxr    %w1, %w0, %2\n"                                          \
"   cbnz    %w1, 1b"                                                 \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i));

#define AUTIL_ATOMIC_OP_RETURN(mb, acq, rel, cl, op, asm_op)         \
    unsigned long tmp;                                               \
    int result;                                                      \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %w0, %2\n"                                       \
"   " #asm_op " %w0, %w0, %w3\n"                                     \
"   st" #rel "xr    %w1, %w0, %2\n"                                  \
"   cbnz    %w1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
                                                                     \
    return result;

#define AUTIL_ATOMIC_OP_RETURN_TEST(mb, acq, rel, cl, op, asm_op)    \
    unsigned long tmp;                                               \
    int result;                                                      \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %w0, %2\n"                                       \
"   " #asm_op " %w0, %w0, %w3\n"                                     \
"   st" #rel "xr    %w1, %w0, %2\n"                                  \
"   cbnz    %w1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
                                                                     \
    return result == 0;

#define AUTIL_ATOMIC_OP_RETURN_NEGATIVE(mb, acq, rel, cl, op, asm_op)    \
    unsigned long tmp;                                               \
    int result;                                                      \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %w0, %2\n"                                       \
"   " #asm_op " %w0, %w0, %w3\n"                                     \
"   st" #rel "xr    %w1, %w0, %2\n"                                  \
"   cbnz    %w1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
	                                                                 \
	return result == 0;

    
//operations used in 64bit atomic counter in aarch64 architecture 

#define AUTIL_ATOMIC64_OP(op, asm_op)                                \
    unsigned long long tmp;                                          \
    long result;                                                     \
                                                                     \
    asm volatile("// atomic_" #op "\n"                               \
"   prfm    pstl1strm, %2\n"                                         \
"1: ldxr    %x0, %2\n"                                               \
"   " #asm_op " %x0, %x0, %x3\n"                                     \
"   stxr    %x1, %x0, %2\n"                                          \
"   cbnz    %x1, 1b"                                                 \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i));

#define AUTIL_ATOMIC64_OP_RETURN(mb, acq, rel, cl, op, asm_op)       \
    unsigned long long tmp;                                          \
    long result;                                                     \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %x0, %2\n"                                       \
"   " #asm_op " %x0, %x0, %x3\n"                                     \
"   st" #rel "xr    %x1, %x0, %2\n"                                  \
"   cbnz    %x1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
                                                                     \
    return result;

#define AUTIL_ATOMIC64_OP_RETURN_TEST(mb, acq, rel, cl, op, asm_op)  \
    unsigned long long tmp;                                          \
    long result;                                                     \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %x0, %2\n"                                       \
"   " #asm_op " %x0, %x0, %x3\n"                                     \
"   st" #rel "xr    %x1, %x0, %2\n"                                  \
"   cbnz    %x1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
                                                                     \
    return result == 0;

#define AUTIL_ATOMIC64_OP_RETURN_NEGATIVE(mb, acq, rel, cl, op, asm_op)\
    unsigned long long tmp;                                          \
    long result;                                                     \
                                                                     \
    asm volatile("// atomic_" #op "_return" "\n"                     \
"   prfm    pstl1strm, %2\n"                                         \
"1: ld" #acq "xr    %x0, %2\n"                                       \
"   " #asm_op " %x0, %x0, %x3\n"                                     \
"   st" #rel "xr    %x1, %x0, %2\n"                                  \
"   cbnz    %x1, 1b\n"                                               \
"   " #mb                                                            \
    : "=&r" (result), "=&r" (tmp), "+Q" (_counter)                   \
    : "Ir" (i)                                                       \
    : cl);                                                           \
                                                                     \
    return result < 0;

#endif

class AtomicCounter
{
public:
    AtomicCounter() {
        _counter = 0;
    }

    inline int getValue() const
    {
        return _counter;
    }

    inline void setValue(int c)
    {
        _counter = c;
    }

    inline void add(int i)
    {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "addl %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
#elif __aarch64__
        AUTIL_ATOMIC_OP(add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void sub(int i)
    {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "subl %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
#elif __aarch64__
        AUTIL_ATOMIC_OP(sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline int addAndReturn(int i)
    {
#if __x86_64__
        int ret = i;
        asm volatile(
            AUTIL_LOCK "xaddl %0, %1"
            :"+r" (i), "+m" (_counter)
            : : "memory");
        return i + ret;
#elif __aarch64__
        AUTIL_ATOMIC_OP_RETURN(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline int subAndReturn(int i)
    {
        return addAndReturn(-i);
    }

    inline int subAndTest(int i)
    {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "subl %2,%0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"ir" (i), "m" (_counter) : "memory");
        return c;
#elif __aarch64__
        AUTIL_ATOMIC_OP_RETURN_TEST(dmb ish,  , l, "memory", sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline void inc() {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "incl %0"
            :"=m" (_counter)
            :"m" (_counter));
#elif __aarch64__
        int i = 1;
        AUTIL_ATOMIC_OP(add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void dec() {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "decl %0"
            :"=m" (_counter)
            :"m" (_counter));
#elif __aarch64__
        int i = 1;
        AUTIL_ATOMIC_OP(sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline bool incAndTest() {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "incl %0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"m" (_counter) : "memory");
        return c != 0;
#elif __aarch64__
        int i = 1;
        AUTIL_ATOMIC_OP_RETURN_TEST(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline bool decAndTest() {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "decl %0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"m" (_counter) : "memory");
        return c != 0;
#elif __aarch64__
        int i = 1;
        AUTIL_ATOMIC_OP_RETURN_TEST(dmb ish,  , l, "memory", sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline int addNegative(int i) {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "addl %2,%0; sets %1"
            :"=m" (_counter), "=qm" (c)
            :"ir" (i), "m" (_counter) : "memory");
        return c;
#elif __aarch64__
        AUTIL_ATOMIC_OP_RETURN_NEGATIVE(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void clearMask(int mask) {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "andl %0,%1"
            : : "r" (~(mask)),"m" (_counter) : "memory");
#elif __aarch64__
        int i = mask;
        AUTIL_ATOMIC_OP(and, and);
#else
        #error arch unsupported!
#endif
    }

    inline void setMask(int mask) {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "orl %0,%1"
            : : "r" (mask),"m" (_counter) : "memory");
#elif __aarch64__
        int i = mask;
        AUTIL_ATOMIC_OP(or, or);
#else
        #error arch unsupported!
#endif
    }

    inline int incAndReturn() {
        return addAndReturn(1);
    }

    inline int decAndReturn() {
        return subAndReturn(1);
    }

protected:
    volatile int _counter;
};

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
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "addq %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
#elif __aarch64__
        AUTIL_ATOMIC64_OP(add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void sub(long i)
    {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "subq %1,%0"
            :"=m" (_counter)
            :"ir" (i), "m" (_counter));
#elif __aarch64__
        AUTIL_ATOMIC64_OP(sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline long addAndReturn(long i)
    {
#if __x86_64__
        long ret = i;
        asm volatile(
            AUTIL_LOCK "xaddq %0, %1"
            :"+r" (i), "+m" (_counter)
            : : "memory");
        return i + ret;
#elif __aarch64__
        AUTIL_ATOMIC64_OP_RETURN(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline long subAndReturn(long i)
    {
        return addAndReturn(-i);
    }

    inline long subAndTest(long i)
    {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "subq %2,%0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"ir" (i), "m" (_counter) : "memory");
        return c;
#elif __aarch64__
        AUTIL_ATOMIC64_OP_RETURN_TEST(dmb ish,  , l, "memory", sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline void inc() {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "incq %0"
            :"=m" (_counter)
            :"m" (_counter));
#elif __aarch64__
        long i = 1;
        AUTIL_ATOMIC64_OP(add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void dec() {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "decq %0"
            :"=m" (_counter)
            :"m" (_counter));
#elif __aarch64__
        long i = 1;
        AUTIL_ATOMIC64_OP(sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline bool incAndTest() {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "incq %0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"m" (_counter) : "memory");
        return c != 0;
#elif __aarch64__
        long i = 1;
        AUTIL_ATOMIC64_OP_RETURN_TEST(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline bool decAndTest() {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "decq %0; sete %1"
            :"=m" (_counter), "=qm" (c)
            :"m" (_counter) : "memory");
        return c != 0;
#elif __aarch64__
        long i = 1;
        AUTIL_ATOMIC64_OP_RETURN_TEST(dmb ish,  , l, "memory", sub, sub);
#else
        #error arch unsupported!
#endif
    }

    inline long addNegative(long i) {
#if __x86_64__
        unsigned char c;
        asm volatile(
            AUTIL_LOCK "addq %2,%0; sets %1"
            :"=m" (_counter), "=qm" (c)
            :"ir" (i), "m" (_counter) : "memory");
        return c;
#elif __aarch64__
        AUTIL_ATOMIC64_OP_RETURN_NEGATIVE(dmb ish,  , l, "memory", add, add);
#else
        #error arch unsupported!
#endif
    }

    inline void clearMask(long mask) {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "andq %0,%1"
            : : "r" (~(mask)),"m" (_counter) : "memory");
#elif __aarch64__
        long i = mask;
        AUTIL_ATOMIC64_OP(and, and);
#else
        #error arch unsupported!
#endif
    }

    inline void setMask(long mask) {
#if __x86_64__
        asm volatile(
            AUTIL_LOCK "orq %0,%1"
            : : "r" (mask),"m" (_counter) : "memory");
#elif __aarch64__
        long i = mask;
        AUTIL_ATOMIC64_OP(or, or);
#else
        #error arch unsupported!
#endif
    }

    inline int incAndReturn() {
        return addAndReturn((long)1);
    }

    inline int decAndReturn() {
        return subAndReturn((long)1);
    }

protected:
    volatile long _counter;
};

#undef AUTIL_LOCK
}




