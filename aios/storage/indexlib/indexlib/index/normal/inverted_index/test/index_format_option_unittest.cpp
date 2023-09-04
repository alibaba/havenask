#include "indexlib/index/normal/inverted_index/test/index_format_option_unittest.h"

#include "indexlib/config/date_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/index_base/schema_adapter.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib::index {
IE_LOG_SETUP(index, IndexFormatOptionTest);

IndexFormatOptionTest::IndexFormatOptionTest() {}

IndexFormatOptionTest::~IndexFormatOptionTest() {}

void IndexFormatOptionTest::CaseSetUp()
{
    IndexPartitionSchemaPtr partitionSchema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "default_index_engine_example.json");
    ASSERT_TRUE(partitionSchema);
    mIndexSchema = partitionSchema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(mIndexSchema);
}

void IndexFormatOptionTest::CaseTearDown() {}

void IndexFormatOptionTest::TestStringIndexForDefaultValue()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("user_name");
    INDEXLIB_TEST_TRUE(!option.HasSectionAttribute());
    INDEXLIB_TEST_TRUE(!option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestTextIndexForDefaultValue()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("title");
    INDEXLIB_TEST_TRUE(!option.HasSectionAttribute());
    // here we add bitmap to text index config which not default config
    INDEXLIB_TEST_TRUE(option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestPackIndexForDefaultValue()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("phrase");
    INDEXLIB_TEST_TRUE(option.HasSectionAttribute());
    INDEXLIB_TEST_TRUE(!option.HasBitmapIndex());
    INDEXLIB_TEST_TRUE(!option.HasTermPayload());
}

void IndexFormatOptionTest::TestCopyConstructor()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("phrase");
    LegacyIndexFormatOption option2 = option;
    INDEXLIB_TEST_TRUE(option.HasSectionAttribute());
}

void IndexFormatOptionTest::TestTfBitmap()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("phrase3");
    ASSERT_TRUE(option.HasTermPayload());
    ASSERT_TRUE(option.HasPositionPayload());
    ASSERT_TRUE(option.HasDocPayload());
}

LegacyIndexFormatOption IndexFormatOptionTest::GetIndexFormatOption(const std::string& indexName)
{
    IndexConfigPtr indexConfig = mIndexSchema->GetIndexConfig(indexName);
    // ASSERT_* can be only used in void function
    EXPECT_TRUE(indexConfig);

    LegacyIndexFormatOption option;
    option.Init(indexConfig);
    return option;
}

void IndexFormatOptionTest::TestToStringFromString()
{
    LegacyIndexFormatOption option = GetIndexFormatOption("phrase3");
    string str = LegacyIndexFormatOption::ToString(option);
    LegacyIndexFormatOption option2 = LegacyIndexFormatOption::FromString(str);
    string str2 = LegacyIndexFormatOption::ToString(option2);
    ASSERT_EQ(str, str2);
}

void IndexFormatOptionTest::TestDateIndexConfig()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "date_index_schema.json");
    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("date_index");
    ASSERT_TRUE(indexConfig);
    DateIndexConfigPtr dateConfig = DYNAMIC_POINTER_CAST(DateIndexConfig, indexConfig);
    ASSERT_EQ(of_none, dateConfig->GetOptionFlag());
    LegacyIndexFormatOption option;
    option.Init(indexConfig);
    ASSERT_FALSE(option.HasSectionAttribute());
    ASSERT_FALSE(option.HasBitmapIndex());
    ASSERT_FALSE(option.HasPositionPayload());
    const PostingFormatOption& postingOption = option.GetPostingFormatOption();
    ASSERT_FALSE(postingOption.HasTfBitmap());
    ASSERT_FALSE(postingOption.HasPositionList());
    ASSERT_FALSE(postingOption.HasTermFrequency());
}

} // namespace indexlib::index
