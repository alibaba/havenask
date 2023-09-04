#include "indexlib/index/common/field_format/range/RangeFieldEncoder.h"

#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class RangeFieldEncoderTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    RangeFieldEncoderTest();
    ~RangeFieldEncoderTest();

    DECLARE_CLASS_NAME(RangeFieldEncoderTest);

public:
    typedef RangeFieldEncoder::Ranges Ranges;
    typedef RangeFieldEncoder::Range Range;
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestTopLevel();
    void TestBigNumber();

private:
    Range MakeRange(uint64_t leftTerm, uint64_t rightTerm, size_t level);
    void AssertEncode(uint64_t leftTerm, uint64_t rightTerm, const Ranges& bottomLevelRanges,
                      const Ranges& higherLevelRanges);
};

INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestTopLevel);
INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestBigNumber);

RangeFieldEncoderTest::RangeFieldEncoderTest() {}

RangeFieldEncoderTest::~RangeFieldEncoderTest() {}

void RangeFieldEncoderTest::CaseSetUp() {}

void RangeFieldEncoderTest::CaseTearDown() {}

void RangeFieldEncoderTest::TestSimpleProcess()
{
    AssertEncode(0, (11UL << 32) - 1, Ranges(), {MakeRange(0, 10, 8)});
    AssertEncode(32, 47, Ranges(), {MakeRange(2, 2, 1)});
    AssertEncode(28, 36, {Range(28, 36)}, Ranges());
    AssertEncode(28, 48, {Range(28, 31), Range(48, 48)}, {MakeRange(2, 2, 1)});
    AssertEncode(1UL << 32, 1UL << 32 | 30, {Range(1UL << 32 | 16, 1UL << 32 | 30)},
                 {MakeRange(1UL << 28, 1UL << 28, 1)});
    AssertEncode(255, 511, {Range(255, 255)}, {MakeRange(1, 1, 2)});
    AssertEncode(240, 256, {Range(256, 256)}, {MakeRange(15, 15, 1)});
    {
        Ranges bottom = {Range(4398046511103L, 4398046511103L), Range(35184372097040L, 35184372097043L)};
        Ranges higher = {
            MakeRange(2199023256064L, 2199023256064L, 1),
            MakeRange(8589934592L, 8589934593L, 3),
            MakeRange(4, 15, 10),
            MakeRange(1, 1, 11),
        };
        sort(higher.begin(), higher.end());
        // ((4 << 40) - 1)  to (2 << 44, 2 << 12, 1 << 4, 3)
        AssertEncode(4398046511103L, 35184372097043L, bottom, higher);
    }
}

void RangeFieldEncoderTest::TestTopLevel()
{
    uint64_t maxNumber = std::numeric_limits<uint64_t>::max();
    AssertEncode(1UL << 60, (3UL << 60) - 1, Ranges(), {MakeRange(1, 2, 15)});
    AssertEncode(0, maxNumber, Ranges(), {MakeRange(0, 15, 15)});
    uint64_t left = (1UL << 60) - 1;
    AssertEncode(left, maxNumber, {Range(left, left)}, {MakeRange(1, 15, 15)});
    Ranges higher;
    for (size_t level = 1; level <= 15; level++) {
        higher.push_back(MakeRange(1, 15, level));
    }
    AssertEncode(1, (uint64_t)(-1), {Range(1, 15)}, higher);
    AssertEncode(0, left, Ranges(), {MakeRange(0, 0, 15)});
    AssertEncode(0, left + 65536, Ranges(), {MakeRange(17592186044416L, 17592186044416L, 4), MakeRange(0, 0, 15)});
}

void RangeFieldEncoderTest::TestBigNumber()
{
    const uint64_t maxNumber = std::numeric_limits<uint64_t>::max();
    uint64_t leftNumber = maxNumber - 5;
    AssertEncode(leftNumber, maxNumber, {Range(leftNumber, maxNumber)}, Ranges());
    leftNumber = maxNumber - 15;
    AssertEncode(leftNumber, maxNumber - 1, {Range(leftNumber, maxNumber - 1)}, Ranges());
    AssertEncode(leftNumber, maxNumber, Ranges(), {MakeRange(maxNumber >> 4, maxNumber >> 4, 1)});
    leftNumber = maxNumber - 65536 - 255;
    AssertEncode(leftNumber, maxNumber, Ranges(),
                 {MakeRange(leftNumber >> 8, leftNumber >> 8, 2), MakeRange(maxNumber >> 16, maxNumber >> 16, 4)});
    uint64_t rightNumber = maxNumber - 4096 * 15;
    leftNumber = maxNumber - 65536 * 2 - 15;
    uint64_t maxSeg = (maxNumber >> 16) - 1;
    AssertEncode(leftNumber, rightNumber, Ranges(),
                 {MakeRange(leftNumber >> 4, leftNumber >> 4, 1), MakeRange(rightNumber >> 12, rightNumber >> 12, 3),
                  MakeRange(maxSeg, maxSeg, 4)});
}

RangeFieldEncoderTest::Range RangeFieldEncoderTest::MakeRange(uint64_t leftTerm, uint64_t rightTerm, size_t level)
{
    return Range(leftTerm | level << 60, rightTerm | level << 60);
}

void RangeFieldEncoderTest::AssertEncode(uint64_t leftTerm, uint64_t rightTerm, const Ranges& bottomLevelRanges,
                                         const Ranges& higherLevelRanges)
{
    Ranges bottom;
    Ranges higher;
    RangeFieldEncoder::CalculateSearchRange(leftTerm, rightTerm, bottom, higher);
    sort(bottom.begin(), bottom.end());
    sort(higher.begin(), higher.end());
    ASSERT_EQ(bottomLevelRanges, bottom);
    ASSERT_EQ(higherLevelRanges, higher);
}

} // namespace indexlib::index
