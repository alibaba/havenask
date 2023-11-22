#pragma once

#include "autil/Log.h"
#include "indexlib/config/build_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(BuildConfigTest, TestEnvBuildTotalMem);
}} // namespace indexlib::config
