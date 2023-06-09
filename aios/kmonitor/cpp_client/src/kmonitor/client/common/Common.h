/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Author Name: zongyuan.wuzy
 * Author Email: zongyuan.wuzy@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_COMMON_COMMON_H_
#define KMONITOR_CLIENT_COMMON_COMMON_H_

#include <stdint.h>
#include <map>
#include <assert.h>
#include <string>
#include <memory>  // NOLINT(build/include_order)
#include "autil/CommonMacros.h"  // NOLINT(build/include_order)
#include <stdlib.h>

#define BEGIN_KMONITOR_NAMESPACE_GLOBAL namespace kmonitor {
#define END_KMONITOR_NAMESPACE_GLOBAL }
#define BEGIN_KMONITOR_NAMESPACE(x) namespace x {
#define END_KMONITOR_NAMESPACE(x) }
#define USE_KMONITOR_NAMESPACE(x) using namespace kmonitor::x  // NOLINT(build/namespaces)
#define KMONITOR_NS(x) kmonitor::x

#define TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#undef LIKELY
#undef UNLIKELY
#undef EXPECTED
/** 逻辑分支跳转预测辅助. */
#if __GNUC__ > 2 || __GNUC_MINOR__ >= 96
    #define LIKELY(x)       __builtin_expect(!!(x), 1)
    #define UNLIKELY(x)     __builtin_expect(!!(x), 0)
    #define EXPECTED(x, y)   __builtin_expect((x), (y))
#else
    #define LIKELY(x)       (x)
    #define UNLIKELY(x)     (x)
    #define EXPECTED(x, y)   (x)
#endif

#ifndef UNUSED
#define UNUSED(variable) (void)(variable)
#endif


#endif  // KMONITOR_CLIENT_COMMON_COMMON_H_
