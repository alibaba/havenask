#ifndef __INDEXLIB_RANGEFIELDENCODERTEST_H
#define __INDEXLIB_RANGEFIELDENCODERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"

IE_NAMESPACE_BEGIN(common);

class RangeFieldEncoderTest : public INDEXLIB_TESTBASE
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
    void AssertEncode(uint64_t leftTerm, uint64_t rightTerm,
                      const Ranges& bottomLevelRanges,
                      const Ranges& higherLevelRanges);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestTopLevel);
INDEXLIB_UNIT_TEST_CASE(RangeFieldEncoderTest, TestBigNumber);
IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RANGEFIELDENCODERTEST_H
