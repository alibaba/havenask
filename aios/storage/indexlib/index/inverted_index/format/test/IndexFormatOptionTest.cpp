#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"
#include "unittest/unittest.h"

using indexlibv2::config::ITabletSchema;

namespace indexlib::index {

class IndexFormatOptionTest : public TESTBASE
{
public:
    void setUp() override
    {
        std::shared_ptr<indexlibv2::config::ITabletSchema> schema =
            indexlibv2::framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(),
                                                                  "default_index_engine_example.json");
        ASSERT_TRUE(schema);
        _schema = schema;
    }
    void tearDown() override {}

private:
    IndexFormatOption GetIndexFormatOption(const std::string& indexName)
    {
        auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
            _schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, indexName));
        // ASSERT_* can be only used in void function
        EXPECT_TRUE(indexConfig);

        IndexFormatOption option;
        option.Init(indexConfig);
        return option;
    }

    std::shared_ptr<ITabletSchema> _schema;
};

TEST_F(IndexFormatOptionTest, testStringIndexForDefaultValue)
{
    IndexFormatOption option = GetIndexFormatOption("user_name");
    ASSERT_TRUE(!option.HasSectionAttribute());
    ASSERT_TRUE(!option.HasBitmapIndex());
    ASSERT_TRUE(!option.HasTermPayload());
}

TEST_F(IndexFormatOptionTest, testTextIndexForDefaultValue)
{
    IndexFormatOption option = GetIndexFormatOption("title");
    ASSERT_TRUE(!option.HasSectionAttribute());
    // here we add bitmap to text index config which not default config
    ASSERT_TRUE(option.HasBitmapIndex());
    ASSERT_TRUE(!option.HasTermPayload());
}

TEST_F(IndexFormatOptionTest, testPackIndexForDefaultValue)
{
    IndexFormatOption option = GetIndexFormatOption("phrase");
    ASSERT_TRUE(option.HasSectionAttribute());
    ASSERT_TRUE(!option.HasBitmapIndex());
    ASSERT_TRUE(!option.HasTermPayload());
}

TEST_F(IndexFormatOptionTest, testCopyConstructor)
{
    IndexFormatOption option = GetIndexFormatOption("phrase");
    IndexFormatOption option2 = option;
    ASSERT_TRUE(option.HasSectionAttribute());
}

TEST_F(IndexFormatOptionTest, testTfBitmap)
{
    IndexFormatOption option = GetIndexFormatOption("phrase3");
    ASSERT_TRUE(option.HasTermPayload());
    ASSERT_TRUE(option.HasPositionPayload());
    ASSERT_TRUE(option.HasDocPayload());
}

TEST_F(IndexFormatOptionTest, testToStringFromString)
{
    IndexFormatOption option = GetIndexFormatOption("phrase3");
    std::string str = IndexFormatOption::ToString(option);
    IndexFormatOption option2 = IndexFormatOption::FromString(str);
    std::string str2 = IndexFormatOption::ToString(option2);
    ASSERT_EQ(str, str2);
}

TEST_F(IndexFormatOptionTest, testDateIndexConfig)
{
    auto tabletSchema =
        indexlibv2::framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "date_index_schema.json");
    ASSERT_TRUE(tabletSchema);
    auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
        tabletSchema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, "date_index"));
    ASSERT_TRUE(indexConfig);
    auto dateConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    ASSERT_TRUE(dateConfig);
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

} // namespace indexlib::index
