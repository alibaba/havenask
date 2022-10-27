#include "indexlib/index/normal/inverted_index/builtin_index/date/test/time_range_info_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TimeRangeInfoTest);

TimeRangeInfoTest::TimeRangeInfoTest()
{
}

TimeRangeInfoTest::~TimeRangeInfoTest()
{
}

void TimeRangeInfoTest::CaseSetUp()
{
}

void TimeRangeInfoTest::CaseTearDown()
{
}

void TimeRangeInfoTest::TestSimpleProcess()
{
    file_system::DirectoryPtr dir = GET_PARTITION_DIRECTORY();
    TimeRangeInfo timeMeta, expectedMeta;
    expectedMeta.mMinTime = 10000;
    expectedMeta.mMaxTime = 99999;
    expectedMeta.Store(dir);
    ASSERT_TRUE(timeMeta.Load(dir));
    ASSERT_EQ(expectedMeta.mMaxTime, timeMeta.mMaxTime);
    ASSERT_EQ(expectedMeta.mMinTime, timeMeta.mMinTime);    
}

IE_NAMESPACE_END(index);

