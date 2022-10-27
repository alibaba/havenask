#ifndef __INDEXLIB_COMMON_H
#define __INDEXLIB_COMMON_H

#define IE_NAMESPACE_BEGIN(x) namespace heavenask { namespace indexlib { namespace x {
#define IE_NAMESPACE_END(x) }}}
#define IE_NAMESPACE_USE(x) using namespace heavenask::indexlib::x

#define IE_NAMESPACE(x) heavenask::indexlib::x

#include <tr1/memory>
#define DEFINE_SHARED_PTR(x) typedef std::tr1::shared_ptr<x> x##Ptr
#define DYNAMIC_POINTER_CAST(x, y) std::tr1::dynamic_pointer_cast<x>(y)

#define CALL_NOT_CONST_FUNC(c, func, args...)   \
    const_cast<c*>(this)->func(args)

#define DECLARE_REFERENCE_CLASS(x, y)           \
    IE_NAMESPACE_BEGIN(x)                       \
    class y;                                    \
    DEFINE_SHARED_PTR(y);                       \
    IE_NAMESPACE_END(x)

#define DECLARE_REFERENCE_STRUCT(x, y)          \
    IE_NAMESPACE_BEGIN(x)                       \
    struct y;                                   \
    DEFINE_SHARED_PTR(y);                       \
    IE_NAMESPACE_END(x)

#define safe_new(type) new (nothrow

#ifdef INDEXLIB_COMMON_PERFTEST
#define MEMORY_BARRIER() {                      \
        __asm__ __volatile__("" ::: "memory");  \
        usleep(1);                              \
    }
#else
#define MEMORY_BARRIER() __asm__ __volatile__("" ::: "memory")
#endif

#ifdef NDEBUG
#define __ALWAYS_INLINE __attribute__((always_inline))
#else
#define __ALWAYS_INLINE
#endif

#define INDEXLIB_CONCATE_IMPL(a,b) a##b
#define INDEXLIB_CONCATE(a,b) INDEXLIB_CONCATE_IMPL(a,b)

#endif
