#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::FieldConfig;
using indexlibv2::config::SingleFieldIndexConfig;
} // namespace

class PostingFormatOptionTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PostingFormatOptionTest, testInit)
{
    std::shared_ptr<SingleFieldIndexConfig> indexConfig(new SingleFieldIndexConfig(std::string("test"), it_string));
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig("fieldName", ft_string, false));
    ASSERT_TRUE(indexConfig->SetFieldConfig(fieldConfig).IsOK());
    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);
}

TEST_F(PostingFormatOptionTest, testEqual)
{
    std::shared_ptr<SingleFieldIndexConfig> indexConfig(new SingleFieldIndexConfig(std::string("test"), it_string));
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig("fieldName", ft_string, false));
    ASSERT_TRUE(indexConfig->SetFieldConfig(fieldConfig).IsOK());
    indexConfig->SetOptionFlag(OPTION_FLAG_ALL);
    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption._dictInlineItemCount = 0;
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption._posListFormatOption.Init(OPTION_FLAG_ALL & ~of_position_payload);
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption._docListFormatOption.Init(OPTION_FLAG_ALL & ~of_doc_payload);
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        ASSERT_TRUE(postingFormatOption == newPostingFormatOption);
    }
}

TEST_F(PostingFormatOptionTest, testJsonize)
{
    PostingFormatOption option(NO_TERM_FREQUENCY);
    JsonizablePostingFormatOption jsonizableOption(option);
    std::string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizablePostingFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr);
    PostingFormatOption convertedOption = convertedJsonizableOption.GetPostingFormatOption();
    ASSERT_EQ(option, convertedOption);
}

TEST_F(PostingFormatOptionTest, testJsonizeCompatibleWithCompressMode)
{
    std::string str = "{"
                      "\"dict_inline_item_count\":"
                      "  3,"
                      "\"doc_list_format_option\":"
                      "  {"
                      "  \"has_doc_payload\":"
                      "    true,"
                      "  \"has_field_map\":"
                      "    false,"
                      "  \"has_term_frequency\":"
                      "    false,"
                      "  \"has_term_frequency_bitmap\":"
                      "    false,"
                      "  \"has_term_frequency_list\":"
                      "    false"
                      "  },"
                      "\"has_term_payload\":"
                      "  true,"
                      "\"is_compressed_posting_header\":"
                      "  true,"
                      "\"pos_list_format_option\":"
                      "  {"
                      "  \"has_position_list\":"
                      "    false,"
                      "  \"has_position_payload\":"
                      "    false,"
                      "  \"has_term_frequency_bitmap\":"
                      "    false"
                      "  }"
                      "}";
    JsonizablePostingFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, str);
    PostingFormatOption convertedOption = convertedJsonizableOption.GetPostingFormatOption();
    ASSERT_EQ(PFOR_DELTA_COMPRESS_MODE, convertedOption.GetDocListCompressMode());
}

TEST_F(PostingFormatOptionTest, testIndexFormatVersion)
{
    std::shared_ptr<SingleFieldIndexConfig> indexConfig(new SingleFieldIndexConfig(std::string("test"), it_string));
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig("fieldName", ft_string, false));
    ASSERT_TRUE(indexConfig->SetFieldConfig(fieldConfig).IsOK());
    ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(1).IsOK());
    indexConfig->SetShortListVbyteCompress(false);

    {
        PostingFormatOption postingFormatOption;
        postingFormatOption.Init(indexConfig);
        JsonizablePostingFormatOption jsonObj(postingFormatOption);
        std::string str = ToJsonString(jsonObj);

        JsonizablePostingFormatOption convertedJsonObj;
        autil::legacy::FromJsonString(convertedJsonObj, str);
        PostingFormatOption newOption = convertedJsonObj.GetPostingFormatOption();
        ASSERT_FALSE(newOption.IsShortListVbyteCompress());
        ASSERT_EQ(1, newOption.GetFormatVersion());
    }

    ASSERT_TRUE(indexConfig->SetIndexFormatVersionId(0).IsOK());
    indexConfig->SetShortListVbyteCompress(true);
    {
        PostingFormatOption postingFormatOption;
        postingFormatOption.Init(indexConfig);
        JsonizablePostingFormatOption jsonObj(postingFormatOption);
        std::string str = ToJsonString(jsonObj);

        JsonizablePostingFormatOption convertedJsonObj;
        autil::legacy::FromJsonString(convertedJsonObj, str);
        PostingFormatOption newOption = convertedJsonObj.GetPostingFormatOption();
        ASSERT_TRUE(newOption.IsShortListVbyteCompress());
        ASSERT_EQ(0, newOption.GetFormatVersion());
    }
}

} // namespace indexlib::index
