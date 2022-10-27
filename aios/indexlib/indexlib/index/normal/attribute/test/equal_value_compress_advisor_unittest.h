#ifndef __INDEXLIB_EQUALVALUECOMPRESSADVISORTEST_H
#define __INDEXLIB_EQUALVALUECOMPRESSADVISORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/equal_value_compress_advisor.h"

IE_NAMESPACE_BEGIN(index);

class EqualValueCompressAdvisorTest : public INDEXLIB_TESTBASE {
public:
    EqualValueCompressAdvisorTest();
    ~EqualValueCompressAdvisorTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetOptSlotItemCount();
    void TestEstimateOptimizeSlotItemCount();
    void TestSampledItemIterator();
private:
    template <typename T>
    void CheckSampledItemIterator(const common::EquivalentCompressReader<T>& reader,
                                  uint32_t sampleRatio, T expectSteps)
    {
        SampledItemIterator<T> iter(reader, sampleRatio);
        T expectValue = 0;
        while(iter.HasNext())
        {
            ASSERT_EQ(expectValue++, iter.Next());
            if (expectValue % 1024 == 0)
            {
		expectValue += expectSteps;
            }
        }
        ASSERT_TRUE(expectValue + expectSteps >= reader.Size());
    }
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EqualValueCompressAdvisorTest, TestGetOptSlotItemCount);
INDEXLIB_UNIT_TEST_CASE(EqualValueCompressAdvisorTest, TestEstimateOptimizeSlotItemCount);
INDEXLIB_UNIT_TEST_CASE(EqualValueCompressAdvisorTest, TestSampledItemIterator);

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_EQUALVALUECOMPRESSADVISORTEST_H 
