#include "indexlib/index/inverted_index/format/PostingFormat.h"

#include "indexlib/index/common/numeric_compress/VbyteInt32Encoder.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class PostingFormatTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PostingFormatTest, testDocListFormat)
{
    PostingFormatOption postingFormatOption(EXPACK_OPTION_FLAG_ALL);
    PostingFormat postingFormat(postingFormatOption);
    DocListFormat* docListFormat = postingFormat.GetDocListFormat();
    ASSERT_TRUE(docListFormat != nullptr);
    ASSERT_TRUE(docListFormat->GetDocIdValue() != nullptr);
    ASSERT_TRUE(docListFormat->GetTfValue() != nullptr);
    ASSERT_TRUE(docListFormat->GetDocPayloadValue() != nullptr);
    ASSERT_TRUE(docListFormat->GetFieldMapValue() != nullptr);
    ASSERT_TRUE(docListFormat->GetDocListSkipListFormat() != nullptr);
}

TEST_F(PostingFormatTest, testPositionListFormat)
{
    {
        // no position list
        PostingFormatOption postingFormatOption(NO_POSITION_LIST);
        PostingFormat postingFormat(postingFormatOption);
        ASSERT_TRUE(postingFormat.GetPositionListFormat() == nullptr);
    }
    {
        // has position list, no position payload
        PostingFormatOption postingFormatOption(of_position_list);
        PostingFormat postingFormat(postingFormatOption);
        PositionListFormat* posListFormat = postingFormat.GetPositionListFormat();
        ASSERT_TRUE(posListFormat != nullptr);
        ASSERT_TRUE(posListFormat->GetPosValue() != nullptr);
        ASSERT_TRUE(posListFormat->GetPositionSkipListFormat() != nullptr);
        ASSERT_TRUE(posListFormat->GetPosPayloadValue() == nullptr);
    }
    {
        // has position list and position payload
        PostingFormatOption postingFormatOption(of_position_list | of_position_payload);
        PostingFormat postingFormat(postingFormatOption);
        PositionListFormat* posListFormat = postingFormat.GetPositionListFormat();
        ASSERT_TRUE(posListFormat != nullptr);
        ASSERT_TRUE(posListFormat->GetPosValue() != nullptr);
        ASSERT_TRUE(posListFormat->GetPositionSkipListFormat() != nullptr);
        ASSERT_TRUE(posListFormat->GetPosPayloadValue() != nullptr);
    }
}

TEST_F(PostingFormatTest, testShortListVbyteCompress)
{
    PostingFormatOption postingFormatOption(EXPACK_OPTION_FLAG_ALL);
    postingFormatOption.SetShortListVbyteCompress(true);
    PostingFormat postingFormat(postingFormatOption);

    DocListFormat* docListFormat = postingFormat.GetDocListFormat();
    ASSERT_TRUE(docListFormat != nullptr);
    ASSERT_TRUE(docListFormat->GetDocIdValue() != nullptr);

    auto encoder = docListFormat->GetDocIdValue()->GetEncoder(SHORT_LIST_COMPRESS_MODE);
    ASSERT_TRUE(encoder);
    // TODO(makuo.mnb) comment temporary, when atomic_value_typed.h migrated, uncomment this
    // ASSERT_TRUE(dynamic_cast<const VbyteInt32Encoder*>(encoder));
}

TEST_F(PostingFormatTest, testIndexFormatVersionIsOne)
{
    PostingFormatOption postingFormatOption(EXPACK_OPTION_FLAG_ALL);
    postingFormatOption.SetShortListVbyteCompress(true);
    postingFormatOption.SetFormatVersion(1);

    PostingFormat postingFormat(postingFormatOption);
    DocListFormat* docListFormat = postingFormat.GetDocListFormat();
    ASSERT_TRUE(docListFormat != nullptr);
    ASSERT_TRUE(docListFormat->GetDocIdValue() != nullptr);

    {
        auto encoder = docListFormat->GetDocIdValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int32PForDeltaEncoderBlockOpt.get());
    }

    {
        auto encoder = docListFormat->GetTfValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int32PForDeltaEncoderBlockOpt.get());
    }

    {
        auto encoder = docListFormat->GetDocPayloadValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int16PForDeltaEncoderBlockOpt.get());
    }

    {
        auto encoder = docListFormat->GetFieldMapValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int8PForDeltaEncoderBlockOpt.get());
    }

    PositionListFormat* posListFormat = postingFormat.GetPositionListFormat();
    ASSERT_TRUE(posListFormat != nullptr);

    {
        auto encoder = posListFormat->GetPosValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int32PForDeltaEncoderBlockOpt.get());
    }
    {
        auto encoder = posListFormat->GetPosPayloadValue()->GetEncoder(PFOR_DELTA_COMPRESS_MODE);
        ASSERT_TRUE(encoder);
        ASSERT_EQ(encoder, EncoderProvider::GetInstance()->_int8PForDeltaEncoderBlockOpt.get());
    }
}

} // namespace indexlib::index
