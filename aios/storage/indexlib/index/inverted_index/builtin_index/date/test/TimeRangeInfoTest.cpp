#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"

#include "indexlib/file_system/Directory.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TimeRangeInfoTest : public TESTBASE
{
public:
    TimeRangeInfoTest();
    ~TimeRangeInfoTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, TimeRangeInfoTest);

TimeRangeInfoTest::TimeRangeInfoTest() {}

TimeRangeInfoTest::~TimeRangeInfoTest() {}

TEST_F(TimeRangeInfoTest, TestSimpleProcess)
{
    auto dir = file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
    ASSERT_TRUE(dir);
    TimeRangeInfo timeMeta, expectedMeta;
    expectedMeta._minTime = 10000;
    expectedMeta._maxTime = 99999;
    ASSERT_TRUE(expectedMeta.Store(dir->GetIDirectory()).IsOK());
    ASSERT_TRUE(timeMeta.Load(dir->GetIDirectory()).IsOK());
    ASSERT_EQ(expectedMeta._maxTime, timeMeta._maxTime);
    ASSERT_EQ(expectedMeta._minTime, timeMeta._minTime);
}
} // namespace indexlib::index
