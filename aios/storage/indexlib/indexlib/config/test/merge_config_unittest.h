#ifndef __INDEXLIB_MERGECONFIGTEST_H
#define __INDEXLIB_MERGECONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class MergeConfigTest : public INDEXLIB_TESTBASE
{
public:
    MergeConfigTest();
    ~MergeConfigTest();

    DECLARE_CLASS_NAME(MergeConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
    void TestDefault();
    void TestCheck();
    void TestPackageFile();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeConfigTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(MergeConfigTest, TestCheck);
INDEXLIB_UNIT_TEST_CASE(MergeConfigTest, TestDefault);
INDEXLIB_UNIT_TEST_CASE(MergeConfigTest, TestPackageFile);
}} // namespace indexlib::config

#endif //__INDEXLIB_MERGECONFIGTEST_H
