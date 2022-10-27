#ifndef __INDEXLIB_AUTOADD2UPDATETEST_H
#define __INDEXLIB_AUTOADD2UPDATETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(partition);

class AutoAdd2updateTest : public INDEXLIB_TESTBASE
{
public:
    AutoAdd2updateTest();
    ~AutoAdd2updateTest();

    DECLARE_CLASS_NAME(AutoAdd2updateTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestIncCoverRt();
    void TestBug_22229643();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AutoAdd2updateTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(AutoAdd2updateTest, TestIncCoverRt);
INDEXLIB_UNIT_TEST_CASE(AutoAdd2updateTest, TestBug_22229643);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_AUTOADD2UPDATETEST_H
