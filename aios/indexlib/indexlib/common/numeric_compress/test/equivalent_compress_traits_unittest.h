#ifndef __INDEXLIB_EQUIVALENTCOMPRESSTRAITSTEST_H
#define __INDEXLIB_EQUIVALENTCOMPRESSTRAITSTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_traits.h"

IE_NAMESPACE_BEGIN(common);

class EquivalentCompressTraitsTest : public INDEXLIB_TESTBASE {
public:
    EquivalentCompressTraitsTest();
    ~EquivalentCompressTraitsTest();
public:
    void SetUp();
    void TearDown();
    void TestZigZagEncode();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EquivalentCompressTraitsTest, TestZigZagEncode);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENTCOMPRESSTRAITSTEST_H
