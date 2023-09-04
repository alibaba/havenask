#include "indexlib/index/normal/inverted_index/builtin_index/date/test/date_index_segment_reader_unittest.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DateIndexSegmentReaderTest);

DateIndexSegmentReaderTest::DateIndexSegmentReaderTest() {}

DateIndexSegmentReaderTest::~DateIndexSegmentReaderTest() {}

void DateIndexSegmentReaderTest::CaseSetUp() {}

void DateIndexSegmentReaderTest::CaseTearDown() {}

void DateIndexSegmentReaderTest::TestNormalizeTimestamp()
{
    DateIndexSegmentReader reader;
    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2011, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2011, 5, 3, 12, 55, 30, 11).GetKey();
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        uint64_t expectLeftTerm = DateTerm(2011, 0, 0, 0, 0, 0, 0).GetKey();
        uint64_t expectRightTerm = DateTerm(2012, 0, 0, 0, 0, 0, 0).GetKey();
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }

    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        uint64_t expectLeftTerm = DateTerm(2010, 0, 0, 0, 0, 0, 0).GetKey();
        uint64_t expectRightTerm = DateTerm(2013, 0, 0, 0, 0, 0, 0).GetKey();
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }

    {
        uint64_t leftTerm = DateTerm(2010, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t rightTerm = DateTerm(2012, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t minTime = DateTerm(2010, 1, 3, 12, 55, 30, 11).GetKey();
        uint64_t maxTime = DateTerm(2013, 2, 3, 12, 55, 30, 11).GetKey();
        uint64_t expectLeftTerm = leftTerm;
        uint64_t expectRightTerm = rightTerm;
        reader.NormalizeTerms(minTime, maxTime, leftTerm, rightTerm);
        ASSERT_EQ(expectLeftTerm, leftTerm);
        ASSERT_EQ(expectRightTerm, rightTerm);
    }
}
}} // namespace indexlib::index
