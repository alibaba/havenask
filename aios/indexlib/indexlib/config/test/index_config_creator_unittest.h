#ifndef __INDEXLIB_INDEXCONFIGCREATORTEST_H
#define __INDEXLIB_INDEXCONFIGCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_config_creator.h"

IE_NAMESPACE_BEGIN(config);

class IndexConfigCreatorTest : public INDEXLIB_TESTBASE
{
public:
    IndexConfigCreatorTest();
    ~IndexConfigCreatorTest();

    DECLARE_CLASS_NAME(IndexConfigCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexConfigCreatorTest, TestSimpleProcess);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEXCONFIGCREATORTEST_H
