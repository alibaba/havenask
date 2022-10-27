#ifndef __INDEXLIB_BUILDCONFIGTEST_H
#define __INDEXLIB_BUILDCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/build_config.h"

IE_NAMESPACE_BEGIN(config);

class BuildConfigTest : public INDEXLIB_TESTBASE
{
public:
    BuildConfigTest();
    ~BuildConfigTest();

    DECLARE_CLASS_NAME(BuildConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
    void TestCheck();
    void TestEnvBuildTotalMem();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestEnvBuildTotalMem);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_BUILDCONFIGTEST_H
