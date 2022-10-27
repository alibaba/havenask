#ifndef __INDEXLIB_REGULAREXPRESSIONTEST_H
#define __INDEXLIB_REGULAREXPRESSIONTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/regular_expression.h"

IE_NAMESPACE_BEGIN(misc);

class RegularExpressionTest : public INDEXLIB_TESTBASE {
public:
    RegularExpressionTest();
    ~RegularExpressionTest();
public:
    void SetUp();
    void TearDown();

    void TestMatch();
    void TestNotInit();
    void TestGetLatestErrMessage();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RegularExpressionTest, TestMatch);
INDEXLIB_UNIT_TEST_CASE(RegularExpressionTest, TestNotInit);
INDEXLIB_UNIT_TEST_CASE(RegularExpressionTest, TestGetLatestErrMessage);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_REGULAREXPRESSIONTEST_H
