#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/attribute_compress_info.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributeCompressInfoTest : public INDEXLIB_TESTBASE
{
public:
    AttributeCompressInfoTest();
    ~AttributeCompressInfoTest();

public:
    void TestNeedCompressData();
    void TestNeedCompressOffset();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeCompressInfoTest, TestNeedCompressData);
INDEXLIB_UNIT_TEST_CASE(AttributeCompressInfoTest, TestNeedCompressOffset);
}} // namespace indexlib::index
