#ifndef __BUILD_SERVICE_UNITTEST_H
#define __BUILD_SERVICE_UNITTEST_H

#include <string>
#include <typeinfo>
#include <execinfo.h>
#include <dlfcn.h>
#include <unistd.h>
#define GTEST_USE_OWN_TR1_TUPLE 0
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "build_service/util/FileUtil.h"
#include "build_service/util/Log.h"
#include "build_service/common/PathDefine.h"
#include "build_service/test/test.h"

#define private public
#define protected public
#define UNIT_TEST UT

using testing::_;
using testing::Return;
using testing::Throw;
using testing::SetArgReferee;
using testing::DoAll;
using testing::Invoke;

class BUILD_SERVICE_TESTBASE : public testing::Test
{
public:
    BUILD_SERVICE_TESTBASE() {}
    virtual ~BUILD_SERVICE_TESTBASE() {}
public:
    void SetUp() {
        testDataPathTearDown();
        testDataPathSetup();
        ::chdir(_testDataPath.c_str());
        setUp();
    }

    void TearDown() {
        tearDown();
        testDataPathTearDown();
    }

public:
    virtual void setUp() {}
    virtual void tearDown() {}

    std::string GET_CLASS_NAME() const
    { return typeid(*this).name(); }

    const std::string& GET_TEST_DATA_PATH() const { return _testDataPath; }

private:
    void testDataPathSetup() {
        std::string tempPath = build_service::util::FileUtil::joinFilePath(
                TEST_DATA_PATH"runtime_testdata/", GET_CLASS_NAME());
        _testDataPath = build_service::util::FileUtil::normalizeDir(tempPath);

        deleteExistDataPath();
        build_service::util::FileUtil::mkDir(_testDataPath, true);
    }

    void testDataPathTearDown() {
        deleteExistDataPath();
    }

    void deleteExistDataPath() {
        bool exist = false;
        if (!_testDataPath.empty()
            && _testDataPath.rfind(TEST_DATA_PATH) != std::string::npos
            && build_service::util::FileUtil::isExist(_testDataPath, exist) && exist)
        {
            build_service::util::FileUtil::remove(_testDataPath);
        }
    }

private:
    std::string _cwd;
    std::string _testDataPath;

protected:
    BS_LOG_DECLARE();
};

#define ASSERT_CAST_AND_RETURN(expectType, pointer)                     \
    ({                                                                  \
        auto p = pointer;                                    \
        ASSERT_TRUE(p) << "pointer is NULL";                            \
        expectType *castResult__ = dynamic_cast<expectType*>(p);        \
        ASSERT_TRUE(castResult__) << "Expect: "#expectType << " Actual: " << typeid(*p).name(); \
        castResult__;                                                   \
    })

// @note:
// This is a link seam helper. It finds the next function with same symbol and
// return. With this helper, you can fake the non-virtual function call easily
// by replace it at runtime.
//
// @param:
// you need to define type *func_type* before call this
//
// for static function, so as the signature:
// ex. typedef ThreadPtr (*func_type)(const std::tr1::function<void ()>& threadFunction);
//
// for class function, lift *this* to parameter list:
// ex. typedef void (*func_type)(ClassType*,  const std::tr1::function<void ()>& );
//
// @restriction:
// 1. you must use it at the function that you fake, since we get backtrace_symbol at depth == 1
// 2. it does not help with *inline* function.. you know
//
// Two way to use it with fake sources:
// 1. compile the fake sources file and generate the shared library by linking to the original library.
// 2. compile the fake sources file, add it to LD_PRELOAD at runtime.
//
// See FakeThread for example.

// the stack backtrace is like:
// /home/lk/test/libtesta.so(_ZN4Test1tESsR2CC+0x1c9) [0x7f7ba79ad1df]

#define GET_NEXT_FUNC(func_type)                                \
    ({                                                          \
        const size_t max_depth = 2;                             \
        void *stack_addrs[max_depth];                           \
                                                                \
        size_t stack_depth = backtrace(stack_addrs, max_depth);         \
        assert(stack_depth == 2 || stack_depth == 1); (void) stack_depth; \
        char **stack_strings = backtrace_symbols(stack_addrs, max_depth); \
                                                                        \
        std::string stackStr = stack_strings[0];                        \
        if (stackStr.find("libasan.so") != std::string::npos) {         \
            stackStr = stack_strings[1];                                \
        }                                                               \
        size_t start = stackStr.find('(');                              \
        assert(start != std::string::npos);                             \
        stackStr = stackStr.substr(start + 1);                          \
        size_t end = stackStr.find('+');                                \
        assert(end != std::string::npos);                               \
        std::string funcSym = stackStr.substr(0, end);                  \
        free(stack_strings);                                            \
        func_type func = reinterpret_cast<func_type>(dlsym(RTLD_NEXT, funcSym.c_str())); \
        assert(func);                                                   \
        func;                                                           \
    })

#endif //__BUILD_SERVICE_UNITTEST_H
