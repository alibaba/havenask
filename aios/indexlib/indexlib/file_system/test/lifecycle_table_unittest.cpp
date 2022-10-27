#include "indexlib/file_system/test/lifecycle_table_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LifecycleTableTest);

LifecycleTableTest::LifecycleTableTest()
{
}

LifecycleTableTest::~LifecycleTableTest()
{
}

void LifecycleTableTest::CaseSetUp()
{
}

void LifecycleTableTest::CaseTearDown()
{
}

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
    
    ASSERT_FALSE(table.AddDirectory("/partition/segment_4_level_2/attribute/", "cold"));
    ASSERT_TRUE(table.AddDirectory("/partition/segment_4_level_2/", "cold"));
    ASSERT_FALSE(table.AddDirectory("/partition/segment_4_level_2/", "hot"));
    
}

IE_NAMESPACE_END(file_system);

