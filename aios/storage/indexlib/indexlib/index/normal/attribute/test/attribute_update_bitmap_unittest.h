#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributeUpdateBitmapTest : public INDEXLIB_TESTBASE
{
public:
    AttributeUpdateBitmapTest();
    ~AttributeUpdateBitmapTest();

    DECLARE_CLASS_NAME(AttributeUpdateBitmapTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeUpdateBitmapTest, TestSimpleProcess);
}} // namespace indexlib::index
