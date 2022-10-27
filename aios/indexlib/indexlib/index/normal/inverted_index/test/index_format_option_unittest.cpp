#include "indexlib/index/normal/inverted_index/test/index_format_option_unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/index_base/schema_adapter.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexFormatOptionTest);

IndexFormatOptionTest::IndexFormatOptionTest()
{
}

IndexFormatOptionTest::~IndexFormatOptionTest()
{
}

void IndexFormatOptionTest::SetUp()
{
    IndexPartitionSchemaPtr partitionSchema = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "default_index_engine_example.json");
    mIndexSchema = partitionSchema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(mIndexSchema);
}

void IndexFormatOptionTest::TearDown()
{
}

void IndexFormatOptionTest::TestStringIndexForDefaultValue()
{
    IndexFormatOption option = GetIndexFormatOption("user_name");
    INDEXLIB_TEST_TRUE(!option.HasSectionAttribute());
    INDEXLIB_TEST_TRUE(!option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestTextIndexForDefaultValue()
{
    IndexFormatOption option = GetIndexFormatOption("title");
    INDEXLIB_TEST_TRUE(!option.HasSectionAttribute());
    //here we add bitmap to text index config which not default config
    INDEXLIB_TEST_TRUE(option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestPackIndexForDefaultValue()
{
    IndexFormatOption option = GetIndexFormatOption("phrase");
    INDEXLIB_TEST_TRUE(option.HasSectionAttribute());
    INDEXLIB_TEST_TRUE(!option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestCopyConstructor()
{
    IndexFormatOption option = GetIndexFormatOption("phrase");
    IndexFormatOption option2 = option;
    INDEXLIB_TEST_TRUE(option.HasSectionAttribute());    
}

void IndexFormatOptionTest::TestTfBitmap()
{
    IndexFormatOption option = GetIndexFormatOption("phrase3");
    ASSERT_TRUE(option.HasTermPayload());
    ASSERT_TRUE(option.HasPositionPayload());
    ASSERT_TRUE(option.HasDocPayload());
}

IndexFormatOption IndexFormatOptionTest::GetIndexFormatOption(const std::string& indexName)
{
    IndexConfigPtr indexConfig = mIndexSchema->GetIndexConfig(indexName);
    // ASSERT_* can be only used in void function
    EXPECT_TRUE(indexConfig);

    IndexFormatOption option;
    option.Init(indexConfig);
    return option;
}

void IndexFormatOptionTest::TestToStringFromString()
{
    IndexFormatOption option = GetIndexFormatOption("phrase3");
    string str = IndexFormatOption::ToString(option);
    IndexFormatOption option2 = IndexFormatOption::FromString(str);
    string str2 = IndexFormatOption::ToString(option2);
    ASSERT_EQ(str, str2);
}

void IndexFormatOptionTest::TestDateIndexConfig()
{
    IndexPartitionSchemaPtr schema  = SchemaAdapter::LoadSchema(
            TEST_DATA_PATH, "date_index_schema.json");
    IndexConfigPtr indexConfig = 
        schema->GetIndexSchema()->GetIndexConfig("date_index");
    ASSERT_TRUE(indexConfig);
    DateIndexConfigPtr dateConfig = 
        DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    ASSERT_EQ(of_none, dateConfig->GetOptionFlag());
    IndexFormatOption option;
    option.Init(indexConfig);
    ASSERT_FALSE(option.HasSectionAttribute());
    ASSERT_FALSE(option.HasBitmapIndex());
    ASSERT_FALSE(option.HasPositionPayload());
    const PostingFormatOption& postingOption = option.GetPostingFormatOption();
    ASSERT_FALSE(postingOption.HasTfBitmap());
    ASSERT_FALSE(postingOption.HasPositionList());
    ASSERT_FALSE(postingOption.HasTermFrequency());
}

IE_NAMESPACE_END(index);

