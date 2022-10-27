#ifndef __INDEXLIB_ATTRIBUTECOMPRESSINFOTEST_H
#define __INDEXLIB_ATTRIBUTECOMPRESSINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"

IE_NAMESPACE_BEGIN(index);

class AttributeCompressInfoTest : public INDEXLIB_TESTBASE {
public:
    AttributeCompressInfoTest();
    ~AttributeCompressInfoTest();
public:
    void SetUp();
    void TearDown();
    void TestNeedCompressData();
    void TestNeedCompressOffset();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeCompressInfoTest, TestNeedCompressData);
INDEXLIB_UNIT_TEST_CASE(AttributeCompressInfoTest, TestNeedCompressOffset);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTECOMPRESSINFOTEST_H
