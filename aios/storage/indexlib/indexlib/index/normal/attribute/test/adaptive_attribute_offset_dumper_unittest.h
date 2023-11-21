#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/data_structure/adaptive_attribute_offset_dumper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class AdaptiveAttributeOffsetDumperTest : public INDEXLIB_TESTBASE
{
public:
    AdaptiveAttributeOffsetDumperTest();
    ~AdaptiveAttributeOffsetDumperTest();

    DECLARE_CLASS_NAME(AdaptiveAttributeOffsetDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AdaptiveAttributeOffsetDumperTest, TestSimpleProcess);
}} // namespace indexlib::index
