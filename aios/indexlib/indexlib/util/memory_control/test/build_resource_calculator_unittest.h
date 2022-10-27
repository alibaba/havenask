#ifndef __INDEXLIB_BUILDRESOURCECALCULATORTEST_H
#define __INDEXLIB_BUILDRESOURCECALCULATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/build_resource_calculator.h"

IE_NAMESPACE_BEGIN(util);

class BuildResourceCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    BuildResourceCalculatorTest();
    ~BuildResourceCalculatorTest();

    DECLARE_CLASS_NAME(BuildResourceCalculatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BuildResourceCalculatorTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BUILDRESOURCECALCULATORTEST_H
