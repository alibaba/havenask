#ifndef __INDEXLIB_VARPARAMTEST_H
#define __INDEXLIB_VARPARAMTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(file_system);

class VarParamTest : public testing::TestWithParam<int>
{
public:
    VarParamTest();
    ~VarParamTest();

//    DECLARE_CLASS_NAME(VarParamTest);
public:
//    void CaseSetUp() override;
//    void CaseTearDown() override;
    void TestSimpleProcess();
public:
    int var;
private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(TestSimpleProcess, VarParamTest, testing::Values(3, 5, 11, 23, 17));

TEST_P(VarParamTest, TestSimpleProcess)
{
    int n = GetParam();
    std::cout << n << std::endl;
    var = n;
    TestSimpleProcess();
}

//INDEXLIB_UNIT_TEST_CASE(VarParamTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_VARPARAMTEST_H
