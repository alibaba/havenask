#pragma once

#include <string>
#include <typeinfo>

#include "unittest/unittest.h"

extern int TEST_ARGC;
extern char** TEST_ARGV;

namespace indexlib {

// INDEXLIB_TESTBASE_BASE
class INDEXLIB_TESTBASE_BASE : public TESTBASE_BASE
{
public:
    INDEXLIB_TESTBASE_BASE()
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string progPath(TEST_ARGV[0]);
        printf("[          ] %s.%s [PROG=%s, PID=%d]\n", curTest->test_case_name(), curTest->name(),
               progPath.substr(progPath.rfind("/") + 1).c_str(), getpid());
        fflush(stdout);
    }
    ~INDEXLIB_TESTBASE_BASE() = default;

public:
    // for indexlib ut, call by INDEXLIB_TESTBASE_BASE::setUp & INDEXLIB_TESTBASE_BASE::tearDown
    virtual void CaseSetUp() {}
    virtual void CaseTearDown() {}
    virtual std::string GET_CLASS_NAME() const { return typeid(*this).name(); }

public:
    std::string GET_TEST_NAME() const
    {
        const ::testing::TestInfo* curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        return std::string(curTest->test_case_name()) + "." + curTest->name();
    }
    const std::string& GET_TEMP_DATA_PATH() const { return TESTBASE_BASE::GET_TEMP_DATA_PATH(); }
    std::string GET_TEMP_DATA_PATH(const std::string& relativePath) const
    {
        return TESTBASE_BASE::GET_TEMP_DATA_PATH() + relativePath;
    }
    std::string GET_NON_RAMDISK_PATH() const { return "/tmp/" + GET_TEMP_DATA_PATH(); }

private:
    // derived from TESTBASE_BASE, call by testing::Test::SetUp & testing::Test::TearDown
    void setUp() override final
    {
        setenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE", "true", 1);
        CaseSetUp();
    }
    void tearDown() override final
    {
        CaseTearDown();
        unsetenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE");
    }
};

// INDEXLIB_TESTBASE
class INDEXLIB_TESTBASE : public INDEXLIB_TESTBASE_BASE, public testing::Test
{
    // derived from testing::Test, call by testing framework
    void SetUp() override final { TESTBASE_BASE::SetUp(); }
    void TearDown() override final { TESTBASE_BASE::TearDown(); }
};

// INDEXLIB_TESTBASE_WITH_PARAM
template <class T>
class INDEXLIB_TESTBASE_WITH_PARAM : public INDEXLIB_TESTBASE_BASE, public testing::TestWithParam<T>
{
    // derived from testing::TestWithParam, call by testing framework
    void SetUp() override final { TESTBASE_BASE::SetUp(); }
    void TearDown() override final { TESTBASE_BASE::TearDown(); }
};

} // namespace indexlib

//////////////////////////////////////////////////////////////////////
#define DECLARE_CLASS_NAME(class_name)                                                                                 \
    std::string GET_CLASS_NAME() const override { return #class_name; }
#define INDEXLIB_UNIT_TEST_CASE(className, caseName)                                                                   \
    TEST_F(className, caseName) { caseName(); }

#define INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(className, caseName)                                                        \
    TEST_P(className, caseName) { caseName(); }
#define INDEXLIB_INSTANTIATE_PARAM_NAME(name, className)                                                               \
    class name : public className                                                                                      \
    {                                                                                                                  \
    };
#define INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(name, className, param)                                                   \
    INDEXLIB_INSTANTIATE_PARAM_NAME(name, className)                                                                   \
    INSTANTIATE_TEST_CASE_P(className, name, param)
#define GET_CASE_PARAM() GetParam()
#define GET_PARAM_VALUE(idx) std::get<(idx)>(GET_CASE_PARAM())
