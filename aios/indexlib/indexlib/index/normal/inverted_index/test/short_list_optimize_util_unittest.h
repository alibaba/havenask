#ifndef __INDEXLIB_SHORTLISTOPTIMIZEUTILTEST_H
#define __INDEXLIB_SHORTLISTOPTIMIZEUTILTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"

IE_NAMESPACE_BEGIN(index);

class ShortListOptimizeUtilTest : public INDEXLIB_TESTBASE {
public:
    ShortListOptimizeUtilTest();
    ~ShortListOptimizeUtilTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestIsDictInlineCompressMode();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ShortListOptimizeUtilTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ShortListOptimizeUtilTest, TestIsDictInlineCompressMode);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHORTLISTOPTIMIZEUTILTEST_H
