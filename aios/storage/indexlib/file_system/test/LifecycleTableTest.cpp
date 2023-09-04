#include "indexlib/file_system/LifecycleTable.h"

#include "gtest/gtest.h"
#include <iosfwd>

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class LifecycleTableTest : public INDEXLIB_TESTBASE
{
public:
    LifecycleTableTest();
    ~LifecycleTableTest();

    DECLARE_CLASS_NAME(LifecycleTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInsertAndGetProcess();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LifecycleTableTest);

INDEXLIB_UNIT_TEST_CASE(LifecycleTableTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LifecycleTableTest, TestInsertAndGetProcess);

//////////////////////////////////////////////////////////////////////

LifecycleTableTest::LifecycleTableTest() {}

LifecycleTableTest::~LifecycleTableTest() {}

void LifecycleTableTest::CaseSetUp() {}

void LifecycleTableTest::CaseTearDown() {}

void LifecycleTableTest::TestSimpleProcess()
{
    LifecycleTable table;
    ASSERT_TRUE(table.AddDirectory("/partition/segment_3_level_2/", "hot"));
    ASSERT_TRUE(table.AddDirectory("/partition/segment_4_level_2/", "cold"));

    EXPECT_EQ("hot", table.GetLifecycle("/partition/segment_3_level_2/index/pk/data"));
    EXPECT_EQ("cold", table.GetLifecycle("/partition/segment_4_level_2/index/pk/data"));
    EXPECT_EQ("", table.GetLifecycle("/partition/segment_9_level_2/index/pk/data"));

    table.RemoveDirectory("/partition/segment_3_level_2/");
    EXPECT_EQ("", table.GetLifecycle("/partition/segment_3_level_2/index/pk/data"));
    ASSERT_TRUE(table.AddDirectory("/partition/segment_4_level_2/", "cold"));
    ASSERT_TRUE(table.AddDirectory("/partition/segment_4_level_2/", "hot"));
}

void LifecycleTableTest::TestInsertAndGetProcess()
{
    LifecycleTable table;
    ASSERT_TRUE(table.AddDirectory("/partition_xxx/segment_1", "hot"));
    ASSERT_TRUE(table.AddDirectory("/partition_xxx/segment_2", "cold"));
    ASSERT_EQ(string("hot"), table.GetLifecycle("/partition_xxx/segment_1/"));
    ASSERT_EQ(string("cold"), table.GetLifecycle("/partition_xxx/segment_2/"));

    auto jsonStr = autil::legacy::ToJsonString(table);
    LifecycleTable newTable;
    autil::legacy::FromJsonString(newTable, jsonStr);

    ASSERT_EQ(2, newTable.Size());
    auto it = newTable.Begin();
    ASSERT_EQ(string("/partition_xxx/segment_1/"), it->first);
    ASSERT_EQ(string("hot"), it->second);

    it++;
    ASSERT_EQ(string("/partition_xxx/segment_2/"), it->first);
    ASSERT_EQ(string("cold"), it->second);

    it++;
    ASSERT_TRUE(it == newTable.End());
    ASSERT_EQ(newTable, table);
}

}} // namespace indexlib::file_system
