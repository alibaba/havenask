#include "indexlib/index/normal/inverted_index/test/posting_decoder_impl_unittest.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_encoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

using testing::Return;
using testing::_;
using testing::InSequence;
using testing::Sequence;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingDecoderImplTest);

PostingDecoderImplTest::PostingDecoderImplTest()
{
}

PostingDecoderImplTest::~PostingDecoderImplTest()
{
}

void PostingDecoderImplTest::SetUp()
{
}

void PostingDecoderImplTest::TearDown()
{
}

void PostingDecoderImplTest::TestInit()
{
    InnerTest(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTest(OPTION_FLAG_ALL, 10);
    InnerTest(NO_POSITION_LIST, 10);
    InnerTest(NO_TERM_FREQUENCY, 10);
    InnerTest(OPTION_FLAG_ALL & ~of_doc_payload, 10);
    InnerTest(OPTION_FLAG_ALL & ~of_position_payload, 10);
}

void PostingDecoderImplTest::TestInitForDictInlineCompress()
{
    TermMeta termMeta;
    PostingFormatOption formatOption(EXPACK_NO_POSITION_LIST);
    PostingDecoderImpl decoder(formatOption);
    uint64_t compressValue = 0;

    DictInlineFormatter formatter(formatOption);
    formatter.SetTermPayload(6);
    formatter.SetDocId(2);
    formatter.SetDocPayload(3);
    formatter.SetTermFreq(4);
    formatter.SetFieldMap(5);
    formatter.GetDictInlineValue(compressValue);
    
    dictvalue_t dictInlineValue = 0;
    dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(compressValue);
    
    decoder.Init(&termMeta, dictInlineValue);
    ASSERT_EQ(formatter.GetDocFreq(), termMeta.GetDocFreq());
    ASSERT_EQ(formatter.GetTermPayload(), termMeta.GetPayload());
    ASSERT_EQ(formatter.GetTermFreq(), termMeta.GetTotalTermFreq());
    ASSERT_TRUE(decoder.mInlineDictValue != INVALID_DICT_VALUE);
}

void PostingDecoderImplTest::TestDecodeDictInlineDocList()
{
    TermMeta termMeta;
    PostingFormatOption formatOption(EXPACK_NO_POSITION_LIST);
    PostingDecoderImpl decoder(formatOption);
    DictInlineFormatter formatter(formatOption);
    formatter.SetTermPayload(6);
    formatter.SetDocId(2);
    formatter.SetDocPayload(3);
    formatter.SetTermFreq(4);
    formatter.SetFieldMap(5);

    uint64_t compressValue = 0;
    formatter.GetDictInlineValue(compressValue);

    dictvalue_t dictInlineValue = 0;
    dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(compressValue);
    
    decoder.Init(&termMeta, dictInlineValue);

    docid_t docId;
    docpayload_t docPayload;
    tf_t termFreq;
    fieldmap_t fieldMap;

    uint32_t decodedDocNum = decoder.DecodeDocList(
            &docId, &termFreq, &docPayload, &fieldMap, 128);
    ASSERT_EQ((uint32_t)1, decodedDocNum);
    ASSERT_EQ((tf_t)4, termFreq);
    ASSERT_EQ((docpayload_t)3, docPayload);
    ASSERT_EQ((fieldmap_t)5, fieldMap);
}

void PostingDecoderImplTest::InnerTest(
        optionflag_t optionFlag,
        df_t df, bool hasTfBitmap)
{
    PostingFormatOption formatOption(optionFlag);
    TermMetaPtr termMeta(new TermMeta(df, 1));
    ByteSliceReaderPtr postingListReader;
    ByteSliceReaderPtr positionListReader;
    BitmapPtr tfBitmap;
    if (hasTfBitmap)
    {
        tfBitmap.reset(new Bitmap());
    }

    PostingDecoderImpl decoder(formatOption);
    decoder.Init(termMeta.get(), postingListReader, positionListReader, tfBitmap);

    ASSERT_TRUE(decoder.mDocIdEncoder != NULL);
    ASSERT_EQ(formatOption.HasTfList(), decoder.mTFListEncoder != NULL);
    ASSERT_EQ(formatOption.HasDocPayload(), decoder.mDocPayloadEncoder != NULL);
    ASSERT_EQ(formatOption.HasFieldMap(), decoder.mFieldMapEncoder != NULL);
    ASSERT_EQ(formatOption.HasPositionList(), decoder.mPositionEncoder != NULL);
    ASSERT_EQ(formatOption.HasPositionPayload(), decoder.mPosPayloadEncoder != NULL);
}

void PostingDecoderImplTest::TestMissPositionBitmap()
{
    ASSERT_THROW(InnerTest(OPTION_FLAG_ALL | of_tf_bitmap, 10, false),
                 InconsistentStateException);
}

void PostingDecoderImplTest::TestDecodeDocList()
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(NO_TERM_FREQUENCY, 10);
    InnerTestDecodeDocList(OPTION_FLAG_ALL & ~of_doc_payload, 10);
}


void PostingDecoderImplTest::TestDocListCollapsed()
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10, false);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_term_frequency, 10, false);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_term_frequency & ~of_doc_payload, 10, false);
}

void PostingDecoderImplTest::TestDecodePosList()
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_position_payload, 10);
}

void PostingDecoderImplTest::TestPosListCollapsed()
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10, false);
}

void PostingDecoderImplTest::InnerTestDecodePosList(
        optionflag_t optionFlag, uint32_t posNum, 
        bool collapse)
{
    PostingFormatOption formatOption(optionFlag);
    if (!formatOption.HasPositionList())
    {
        return;
    }

    Int32EncoderPtr posListEncoder;
    Int8EncoderPtr posPayloadEncoder;

    PostingDecoderImpl decoder(formatOption);
    TermMetaPtr termMeta(new TermMeta(1, posNum));
    decoder.mTermMeta = termMeta.get();

    size_t retLen = posNum;
    Sequence s;
    MockInt32Encoder * encoder = new MockInt32Encoder();
    EXPECT_CALL(*encoder, Decode(_,(size_t)posNum,_))
        .InSequence(s)
        .WillOnce(Return(retLen));

    decoder.mPositionEncoder = encoder;
    posListEncoder.reset(encoder);

    if (formatOption.HasPositionPayload())
    {
        if (collapse) { retLen++; }
        MockInt8Encoder * encoder = new MockInt8Encoder();
        EXPECT_CALL(*encoder, Decode(_,(size_t)posNum,_))
            .InSequence(s)
            .WillOnce(Return(retLen));

        decoder.mPosPayloadEncoder = encoder;
        posPayloadEncoder.reset(encoder);
    }

    if (collapse)
    {
        ASSERT_THROW(decoder.DecodeDocList(NULL, NULL, NULL, NULL, posNum),
                     IndexCollapsedException);
    }
    else
    {
        decoder.DecodeDocList(NULL, NULL, NULL, NULL, posNum);
        ASSERT_EQ((df_t)posNum, decoder.mDecodedPosCount);
    }
}

void PostingDecoderImplTest::InnerTestDecodeDocList(
        optionflag_t optionFlag, uint32_t docNum,
        bool collapse)
{
    Int32EncoderPtr docIdEncoder;
    Int32EncoderPtr tfListEncoder;
    Int16EncoderPtr docPayloadEncoder;
    Int8EncoderPtr fieldMapEncoder;

    TermMetaPtr termMeta(new TermMeta(docNum, 1));
    PostingFormatOption formatOption(optionFlag);
    PostingDecoderImpl decoder(formatOption);
    decoder.mTermMeta = termMeta.get();

    size_t retLen = docNum;
    Sequence s;
    MockInt32Encoder * encoder = new MockInt32Encoder();
    EXPECT_CALL(*encoder, Decode(_,(size_t)docNum,_))
        .InSequence(s)
        .WillOnce(Return(retLen));

    decoder.mDocIdEncoder = encoder;
    docIdEncoder.reset(encoder);

    if (formatOption.HasTfList())
    {
        if (collapse) { retLen++; }
        MockInt32Encoder * encoder = new MockInt32Encoder();
        EXPECT_CALL(*encoder, Decode(_,(size_t)docNum,_))
            .InSequence(s)
            .WillOnce(Return(retLen));

        decoder.mTFListEncoder = encoder;
        tfListEncoder.reset(encoder);
    }

    if (formatOption.HasDocPayload())
    {
        if (collapse) { retLen++; }
        MockInt16Encoder * encoder = new MockInt16Encoder();
        EXPECT_CALL(*encoder, Decode(_,(size_t)docNum,_))
            .InSequence(s)
            .WillOnce(Return(retLen));

        decoder.mDocPayloadEncoder = encoder;
        docPayloadEncoder.reset(encoder);
    }

    if (formatOption.HasFieldMap())
    {
        if (collapse) { retLen++; }
        MockInt8Encoder * encoder = new MockInt8Encoder();
        EXPECT_CALL(*encoder, Decode(_,(size_t)docNum,_))
            .InSequence(s)
            .WillOnce(Return(retLen));

        decoder.mFieldMapEncoder = encoder;
        fieldMapEncoder.reset(encoder);
    }

    if (collapse)
    {
        ASSERT_THROW(decoder.DecodeDocList(NULL, NULL, NULL, NULL, docNum),
                     IndexCollapsedException);
    }
    else
    {
        decoder.DecodeDocList(NULL, NULL, NULL, NULL, docNum);
        ASSERT_EQ((df_t)docNum, decoder.mDecodedDocCount);
    }
}

IE_NAMESPACE_END(index);

