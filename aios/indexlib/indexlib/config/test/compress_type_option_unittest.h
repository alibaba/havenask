#ifndef __INDEXLIB_COMPRESSTYPEOPTIONTEST_H
#define __INDEXLIB_COMPRESSTYPEOPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(config);

class CompressTypeOptionTest : public INDEXLIB_TESTBASE {
public:
    CompressTypeOptionTest();
    ~CompressTypeOptionTest();
public:
    void SetUp();
    void TearDown();
    void TestInit();
    void TestGetCompressStr();
    void TestAssertEqual();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressTypeOptionTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(CompressTypeOptionTest, TestGetCompressStr);
INDEXLIB_UNIT_TEST_CASE(CompressTypeOptionTest, TestAssertEqual);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_COMPRESSTYPEOPTIONTEST_H
