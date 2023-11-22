#pragma once

#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class SectionAttributeFormatterTest : public INDEXLIB_TESTBASE
{
public:
    SectionAttributeFormatterTest();
    ~SectionAttributeFormatterTest();

public:
    void TestSimpleProcess();
    void TestUnpackBuffer();

private:
    SectionAttributeFormatterPtr CreateFormatter(bool hasFieldId, bool hasWeight);

    void CheckBuffer(uint8_t* buffer, uint32_t sectionCount, const section_len_t* lenBuffer,
                     const section_fid_t* fidBuffer, const section_weight_t* weightBuffer);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeFormatterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeFormatterTest, TestUnpackBuffer);
}} // namespace indexlib::common
