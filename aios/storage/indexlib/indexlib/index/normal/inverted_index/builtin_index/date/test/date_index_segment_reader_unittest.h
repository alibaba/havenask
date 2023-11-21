#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/builtin_index/date/date_index_segment_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DateIndexSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    DateIndexSegmentReaderTest();
    ~DateIndexSegmentReaderTest();

    DECLARE_CLASS_NAME(DateIndexSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalizeTimestamp();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DateIndexSegmentReaderTest, TestNormalizeTimestamp);
}} // namespace indexlib::index
