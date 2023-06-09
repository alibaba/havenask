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

#define DELETE_AND_SET_NULL(x)                                                                                         \
    do {                                                                                                               \
        if ((x) != NULL) {                                                                                             \
            delete (x);                                                                                                \
            (x) = NULL;                                                                                                \
        }                                                                                                              \
    } while (false)

#define ARRAY_DELETE_AND_SET_NULL(x)                                                                                   \
    do {                                                                                                               \
        if ((x) != NULL) {                                                                                             \
            delete[](x);                                                                                               \
            (x) = NULL;                                                                                                \
        }                                                                                                              \
    } while (false)

#ifndef likely
#define likely(x) __builtin_expect((x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect((x), 0)
#endif
//////////////////////////// copy from protobuf/stubs/common.h ///////////////////////
// The COMPILE_ASSERT macro can be used to verify that a compile time
// expression is true. For example, you could use it to verify the
// size of a static array:
//
//   COMPILE_ASSERT(ARRAYSIZE(content_type_names) == CONTENT_NUM_TYPES,
//                  content_type_names_incorrect_size);
//
// or to make sure a struct is smaller than a certain size:
//
//   COMPILE_ASSERT(sizeof(foo) < 128, foo_too_large);
//
// The second argument to the macro is the name of the variable. If
// the expression is false, most compilers will issue a warning/error
// containing the name of the variable.

#if defined(__cplusplus) && (__cplusplus >= 201703L)
#define AUTIL_NODISCARD [[nodiscard]]
#else
#define AUTIL_NODISCARD __attribute__((warn_unused_result))
#endif

// need add #include <memory> in your code file
#define AUTIL_DECLARE_CLASS_SHARED_PTR(ns, c)                                                                          \
    namespace ns {                                                                                                     \
    class c;                                                                                                           \
    typedef std::shared_ptr<c> c##Ptr;                                                                                 \
    }

#define AUTIL_DECLARE_STRUCT_SHARED_PTR(ns, c)                                                                         \
    namespace ns {                                                                                                     \
    struct c;                                                                                                          \
    typedef std::shared_ptr<c> c##Ptr;                                                                                 \
    }

#ifdef __has_feature
#define AUTIL_HAVE_FEATURE(f) __has_feature(f)
#else
#define AUTIL_HAVE_FEATURE(f) 0
#endif

// AddressSanitizer (ASan) is a fast memory error detector.
#ifdef AUTIL_HAVE_ADDRESS_SANITIZER
#error "AUTIL_HAVE_ADDRESS_SANITIZER cannot be directly set."
#elif defined(__SANITIZE_ADDRESS__)
#define AUTIL_HAVE_ADDRESS_SANITIZER 1
#elif AUTIL_HAVE_FEATURE(address_sanitizer)
#define AUTIL_HAVE_ADDRESS_SANITIZER 1
#endif

// MemorySanitizer (MSan) is a detector of uninitialized reads. It consists of
// a compiler instrumentation module and a run-time library.
#ifdef AUTIL_HAVE_MEMORY_SANITIZER
#error "AUTIL_HAVE_MEMORY_SANITIZER cannot be directly set."
#elif !defined(__native_client__) && AUTIL_HAVE_FEATURE(memory_sanitizer)
#define AUTIL_HAVE_MEMORY_SANITIZER 1
#endif
