#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/test.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/misc/exception.h"
using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);

class DiversityConstrainTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DiversityConstrainTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForParseTimeStampFilter()
    {
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=now;adviseEndTime=23:00:00;minInterval=6";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;    // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.mFilterField);
            INDEXLIB_TEST_EQUAL(7402, constrain.mFilterMinValue);
            INDEXLIB_TEST_EQUAL(82800, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;    // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.mFilterField);
            INDEXLIB_TEST_EQUAL(7200, constrain.mFilterMinValue);
            INDEXLIB_TEST_EQUAL(82800, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByMeta";
            constrain.mFilterParameter = "fieldName=ends_1;beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;    // 1970-1-1
            constrain.ParseTimeStampFilter(beginTime, baseTime);
            int64_t defaultMin = DiversityConstrain::DEFAULT_FILTER_MIN_VALUE;
            int64_t defaultMax = DiversityConstrain::DEFAULT_FILTER_MAX_VALUE;
            INDEXLIB_TEST_EQUAL(string(""), constrain.mFilterField);
            INDEXLIB_TEST_EQUAL(defaultMin, constrain.mFilterMinValue);
            INDEXLIB_TEST_EQUAL(defaultMax, constrain.mFilterMaxValue);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByTimeStamp";
            constrain.mFilterParameter = "beginTime=2:0:0;adviseEndTime=3:0:00;minInterval=21";
            int64_t beginTime = 7402; // 2:3:22
            int64_t baseTime = 0;    // 1970-1-1

            INDEXLIB_EXPECT_EXCEPTION(
                    constrain.ParseTimeStampFilter(beginTime, baseTime), 
                    misc::BadParameterException);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "FilterByMeta";
            constrain.mFilterParameter = "fieldName=ends_1";
            constrain.ParseFilterParam();
            INDEXLIB_TEST_EQUAL(string("ends_1"), constrain.mFilterField);
        }
        {
            DiversityConstrain constrain;
            constrain.mFilterType = "Default";
            constrain.mFilterParameter = "fieldName=DOC_PAYLOAD;filterMask=0xFFFF;filterMinValue=1;filterMaxValue=100";
            constrain.ParseFilterParam();
            INDEXLIB_TEST_EQUAL(string("DOC_PAYLOAD"), constrain.mFilterField);
            INDEXLIB_TEST_EQUAL((uint64_t)0xFFFF, constrain.mFilterMask);
            INDEXLIB_TEST_EQUAL(1l, constrain.mFilterMinValue);
            INDEXLIB_TEST_EQUAL(100l, constrain.mFilterMaxValue);
        }

    }
    
    void TestCaseForGetTimeFromString()
    {
        DiversityConstrain constrain;
        INDEXLIB_TEST_EQUAL(10983, constrain.GetTimeFromString("3:3:3"));
        INDEXLIB_TEST_EQUAL(83106, constrain.GetTimeFromString("23:5:6"));
        INDEXLIB_TEST_EQUAL(0, constrain.GetTimeFromString("0:0:0"));
    }

    void TestCaseForEndTimeLessThanBeginTime()
    {
        DiversityConstrain constrain;
        constrain.mFilterType = "FilterByTimeStamp";
        constrain.mFilterParameter = "beginTime=10:0:0;adviseEndTime=9:0:0;fieldName=ends";
        INDEXLIB_EXPECT_EXCEPTION(constrain.ParseTimeStampFilter(0, 0), 
                misc::BadParameterException);
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, DiversityConstrainTest);

INDEXLIB_UNIT_TEST_CASE(DiversityConstrainTest, TestCaseForParseTimeStampFilter);
INDEXLIB_UNIT_TEST_CASE(DiversityConstrainTest, TestCaseForGetTimeFromString);

IE_NAMESPACE_END(config);
