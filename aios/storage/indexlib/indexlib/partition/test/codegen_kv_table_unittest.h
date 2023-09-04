#ifndef __INDEXLIB_CODEGENKVTABLETEST_H
#define __INDEXLIB_CODEGENKVTABLETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class CodegenKvTableTest : public INDEXLIB_TESTBASE
{
public:
    CodegenKvTableTest();
    ~CodegenKvTableTest();

    DECLARE_CLASS_NAME(CodegenKvTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFixLenKVReader();
    void TestVarLenKVReader();
    void TestCheckCodegenCompactBucket();
    void TestCheckMultiRegion();

private:
    std::string mDisableCodegen;
    std::string mWorkDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CodegenKvTableTest, TestFixLenKVReader);
INDEXLIB_UNIT_TEST_CASE(CodegenKvTableTest, TestVarLenKVReader);
INDEXLIB_UNIT_TEST_CASE(CodegenKvTableTest, TestCheckCodegenCompactBucket);
INDEXLIB_UNIT_TEST_CASE(CodegenKvTableTest, TestCheckMultiRegion);
}} // namespace indexlib::partition

#endif //__INDEXLIB_CODEGENKVTABLETEST_H
