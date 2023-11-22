#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class CodegenKkvTableTest : public INDEXLIB_TESTBASE
{
public:
    CodegenKkvTableTest();
    ~CodegenKkvTableTest();

    DECLARE_CLASS_NAME(CodegenKkvTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMultiRegionCodegen();

private:
    std::string mDisableCodegen;
    std::string mWorkDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CodegenKkvTableTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(CodegenKkvTableTest, TestMultiRegionCodegen);
}} // namespace indexlib::partition
