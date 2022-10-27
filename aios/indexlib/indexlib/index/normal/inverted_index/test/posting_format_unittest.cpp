#include "indexlib/index/normal/inverted_index/test/posting_format_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingFormatTest);

PostingFormatTest::PostingFormatTest()
{
}

PostingFormatTest::~PostingFormatTest()
{
}

void PostingFormatTest::CaseSetUp()
{
}

void PostingFormatTest::CaseTearDown()
{
}

void PostingFormatTest::TestDocListFormat()
{
    PostingFormatOption postingFormatOption(EXPACK_OPTION_FLAG_ALL);
    PostingFormat postingFormat(postingFormatOption);
    DocListFormat* docListFormat = postingFormat.GetDocListFormat();
    ASSERT_TRUE(docListFormat != NULL);
    ASSERT_TRUE(docListFormat->GetDocIdValue() != NULL);
    ASSERT_TRUE(docListFormat->GetTfValue() != NULL);
    ASSERT_TRUE(docListFormat->GetDocPayloadValue() != NULL);
    ASSERT_TRUE(docListFormat->GetFieldMapValue() != NULL);
    ASSERT_TRUE(docListFormat->GetDocListSkipListFormat() != NULL);
}

void PostingFormatTest::TestPositionListFormat()
{
    {
        // no position list
        PostingFormatOption postingFormatOption(NO_POSITION_LIST);
        PostingFormat postingFormat(postingFormatOption);
        ASSERT_TRUE(postingFormat.GetPositionListFormat() == NULL);
    }
    {
        // has position list, no position payload
        PostingFormatOption postingFormatOption(of_position_list);
        PostingFormat postingFormat(postingFormatOption);
        PositionListFormat* posListFormat = postingFormat.GetPositionListFormat();
        ASSERT_TRUE(posListFormat != NULL);
        ASSERT_TRUE(posListFormat->GetPosValue() != NULL);
        ASSERT_TRUE(posListFormat->GetPositionSkipListFormat() != NULL);
        ASSERT_TRUE(posListFormat->GetPosPayloadValue() == NULL);
    }
    {
        // has position list and position payload
        PostingFormatOption postingFormatOption(of_position_list | of_position_payload);
        PostingFormat postingFormat(postingFormatOption);
        PositionListFormat* posListFormat = postingFormat.GetPositionListFormat();
        ASSERT_TRUE(posListFormat != NULL);
        ASSERT_TRUE(posListFormat->GetPosValue() != NULL);
        ASSERT_TRUE(posListFormat->GetPositionSkipListFormat() != NULL);
        ASSERT_TRUE(posListFormat->GetPosPayloadValue() != NULL);
    }
}

IE_NAMESPACE_END(index);

