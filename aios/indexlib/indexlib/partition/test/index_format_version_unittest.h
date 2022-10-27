#ifndef __INDEXLIB_INDEXFORMATVERSIONTEST_H
#define __INDEXLIB_INDEXFORMATVERSIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/index_format_version.h"

IE_NAMESPACE_BEGIN(partition);

class IndexFormatVersionTest : public INDEXLIB_TESTBASE
{
public:
    IndexFormatVersionTest();
    ~IndexFormatVersionTest();

    DECLARE_CLASS_NAME(IndexFormatVersionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCheckCompatible();
    void TestLoadAndStore();
    void TestCompareOperator();    
private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexFormatVersionTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(IndexFormatVersionTest, TestLoadAndStore);
INDEXLIB_UNIT_TEST_CASE(IndexFormatVersionTest, TestCheckCompatible);
INDEXLIB_UNIT_TEST_CASE(IndexFormatVersionTest, TestCompareOperator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXFORMATVERSIONTEST_H
