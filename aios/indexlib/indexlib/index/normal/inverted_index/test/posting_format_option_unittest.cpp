#include "indexlib/index/normal/inverted_index/test/posting_format_option_unittest.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingFormatOptionTest);

PostingFormatOptionTest::PostingFormatOptionTest()
{
}

PostingFormatOptionTest::~PostingFormatOptionTest()
{
}

void PostingFormatOptionTest::CaseSetUp()
{
}

void PostingFormatOptionTest::CaseTearDown()
{
}

void PostingFormatOptionTest::TestInit()
{
    SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig(string("test"), it_string));
    FieldConfigPtr fieldConfig(new FieldConfig("fieldName", ft_string, false));
    indexConfig->SetFieldConfig(fieldConfig);
    indexConfig->SetDictInlineCompress(true);
    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);
    ASSERT_TRUE(postingFormatOption.IsDictInlineCompress());
}

void PostingFormatOptionTest::TestEqual()
{
    SingleFieldIndexConfigPtr indexConfig(new SingleFieldIndexConfig(string("test"), it_string));
    FieldConfigPtr fieldConfig(new FieldConfig("fieldName", ft_string, false));
    indexConfig->SetFieldConfig(fieldConfig);
    indexConfig->SetDictInlineCompress(true);
    indexConfig->SetOptionFlag(OPTION_FLAG_ALL);
    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);
    
    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption.mDictInlineItemCount = 0;
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption.mPosListFormatOption.Init(OPTION_FLAG_ALL & ~of_position_payload);
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }

    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        newPostingFormatOption.mDocListFormatOption.Init(OPTION_FLAG_ALL & ~of_doc_payload);
        ASSERT_FALSE(postingFormatOption == newPostingFormatOption);
    }
    
    {
        PostingFormatOption newPostingFormatOption = postingFormatOption;
        ASSERT_TRUE(postingFormatOption == newPostingFormatOption);
    }
}

void PostingFormatOptionTest::TestJsonize()
{
    PostingFormatOption option(NO_TERM_FREQUENCY);
    JsonizablePostingFormatOption jsonizableOption(option);
    string jsonStr = autil::legacy::ToJsonString(jsonizableOption);
    JsonizablePostingFormatOption convertedJsonizableOption;
    autil::legacy::FromJsonString(convertedJsonizableOption, jsonStr); 
    PostingFormatOption convertedOption = 
        convertedJsonizableOption.GetPostingFormatOption();
    ASSERT_EQ(option, convertedOption);
}

void PostingFormatOptionTest::TestJsonizeCompatibleWithCompressMode()
{
    string str = "{"
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
    PostingFormatOption convertedOption = 
        convertedJsonizableOption.GetPostingFormatOption();
    ASSERT_EQ(index::PFOR_DELTA_COMPRESS_MODE, convertedOption.GetDocListCompressMode());
}
IE_NAMESPACE_END(index);

