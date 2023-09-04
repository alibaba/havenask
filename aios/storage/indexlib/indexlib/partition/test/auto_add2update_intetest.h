#ifndef __INDEXLIB_AUTOADD2UPDATETEST_H
#define __INDEXLIB_AUTOADD2UPDATETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class AutoAdd2updateTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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

INSTANTIATE_TEST_CASE_P(BuildMode, AutoAdd2updateTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AutoAdd2updateTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AutoAdd2updateTest, TestIncCoverRt);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(AutoAdd2updateTest, TestBug_22229643);
}} // namespace indexlib::partition

#endif //__INDEXLIB_AUTOADD2UPDATETEST_H
