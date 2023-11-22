#include "autil/Log.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/testutil/unittest.h"
using namespace std;

namespace indexlib { namespace config {

class DiversityConstrainTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DiversityConstrainTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForParseTimeStampFilter()
    {
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=now;adviseEndTime=23:00:00;minInterval=6";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;     // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            ASSERT_EQ(string("ends_1"), constrain.mFilterField);
            ASSERT_EQ(7402, constrain.mFilterMinValue);
            ASSERT_EQ(82800, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;     // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            ASSERT_EQ(string("ends_1"), constrain.mFilterField);
            ASSERT_EQ(7200, constrain.mFilterMinValue);
            ASSERT_EQ(82800, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByMeta";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;     // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            int64_t defaultMin = DiversityConstrain::DEFAULT_FILTER_MIN_VALUE;
            int64_t defaultMax = DiversityConstrain::DEFAULT_FILTER_MAX_VALUE;
            ASSERT_EQ(string(""), constrain.mFilterField);
            ASSERT_EQ(defaultMin, constrain.mFilterMinValue);
            ASSERT_EQ(defaultMax, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;     // 1970-1-1

            ASSERT_THROW(constrain.ParseTimeStampFilter(beginTime, baseTime), util::BadParameterException);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByMeta";
            constrain.mFilterParameter = "fieldName=ends_1";
            constrain.ParseFilterParam();
            ASSERT_EQ(string("ends_1"), constrain.mFilterField);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "Default";
            constrain.mFilterParameter = "fieldName=DOC_PAYLOAD;filterMask=0xFFFF;filterMinValue=1;filterMaxValue=100";
            constrain.ParseFilterParam();
            ASSERT_EQ(string("DOC_PAYLOAD"), constrain.mFilterField);
            ASSERT_EQ((uint64_t)0xFFFF, constrain.mFilterMask);
            ASSERT_EQ(1l, constrain.mFilterMinValue);
            ASSERT_EQ(100l, constrain.mFilterMaxValue);
        }
    }

    void TestCaseForGetTimeFromString()
    {
        DiversityConstrain constrain;
        ASSERT_EQ(10983, constrain.GetTimeFromString("3:3:3"));
        ASSERT_EQ(83106, constrain.GetTimeFromString("23:5:6"));
        ASSERT_EQ(0, constrain.GetTimeFromString("0:0:0"));
    }

    void TestCaseForEndTimeLessThanBeginTime()
    {
        DiversityConstrain constrain;
        constrain.mFilterType = "FilterByTimeStamp";
        constrain.mFilterParameter = "beginTime=10:0:0;adviseEndTime=9:0:0;fieldName=ends";
        ASSERT_THROW(constrain.ParseTimeStampFilter(0, 0), util::BadParameterException);
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(index, DiversityConstrainTest);

INDEXLIB_UNIT_TEST_CASE(DiversityConstrainTest, TestCaseForParseTimeStampFilter);
INDEXLIB_UNIT_TEST_CASE(DiversityConstrainTest, TestCaseForGetTimeFromString);
}} // namespace indexlib::config
