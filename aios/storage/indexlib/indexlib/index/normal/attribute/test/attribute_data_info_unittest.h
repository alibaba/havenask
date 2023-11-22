#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_info.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AttributeDataInfoTest : public INDEXLIB_TESTBASE
{
public:
    AttributeDataInfoTest();
    ~AttributeDataInfoTest();

    DECLARE_CLASS_NAME(AttributeDataInfoTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeDataInfoTest, TestSimpleProcess);
}} // namespace indexlib::index
