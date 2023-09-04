#include "indexlib/index/inverted_index/format/PostingDecoderImpl.h"

#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/numeric_compress/IntEncoder.h"
#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/util/Bitmap.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using testing::_;
using testing::InSequence;
using testing::Return;
using testing::Sequence;
} // namespace

class PostingDecoderImplTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

    class MockInt32Encoder : public Int32Encoder
    {
    public:
        MOCK_METHOD((std::pair<Status, uint32_t>), Encode, (file_system::ByteSliceWriter&, const uint32_t*, uint32_t),
                    (const, override));
        MOCK_METHOD((std::pair<Status, uint32_t>), Encode, (uint8_t*, const uint32_t*, uint32_t), (const, override));
        MOCK_METHOD((std::pair<Status, uint32_t>), Decode, (uint32_t*, uint32_t, file_system::ByteSliceReader&),
                    (const, override));
    };

    class MockInt16Encoder : public Int16Encoder
    {
    public:
        MOCK_METHOD((std::pair<Status, uint32_t>), Encode, (file_system::ByteSliceWriter&, const uint16_t*, uint32_t),
                    (const, override));
        MOCK_METHOD((std::pair<Status, uint32_t>), Decode, (uint16_t*, uint32_t, file_system::ByteSliceReader&),
                    (const, override));
    };

    class MockInt8Encoder : public Int8Encoder
    {
    public:
        MOCK_METHOD((std::pair<Status, uint32_t>), Encode, (file_system::ByteSliceWriter&, const uint8_t*, uint32_t),
                    (const, override));
        MOCK_METHOD((std::pair<Status, uint32_t>), Decode, (uint8_t*, uint32_t, file_system::ByteSliceReader&),
                    (const, override));
    };

    void InnerTest(optionflag_t optionFlag, df_t df, bool hasTfBitmap = true)
    {
        PostingFormatOption formatOption(optionFlag);
        std::shared_ptr<TermMeta> termMeta(new TermMeta(df, 1));
        file_system::ByteSliceReaderPtr postingListReader;
        file_system::ByteSliceReaderPtr positionListReader;
        std::shared_ptr<util::Bitmap> tfBitmap;
        if (hasTfBitmap) {
            tfBitmap.reset(new util::Bitmap());
        }

        PostingDecoderImpl decoder(formatOption);
        decoder.Init(termMeta.get(), postingListReader, positionListReader, tfBitmap, 20);

        ASSERT_TRUE(decoder._docIdEncoder != nullptr);
        ASSERT_EQ(formatOption.HasTfList(), decoder._tfListEncoder != nullptr);
        ASSERT_EQ(formatOption.HasDocPayload(), decoder._docPayloadEncoder != nullptr);
        ASSERT_EQ(formatOption.HasFieldMap(), decoder._fieldMapEncoder != nullptr);
        ASSERT_EQ(formatOption.HasPositionList(), decoder._positionEncoder != nullptr);
        ASSERT_EQ(formatOption.HasPositionPayload(), decoder._posPayloadEncoder != nullptr);
    }

    void InnerTestDecodePosList(optionflag_t optionFlag, uint32_t posNum, bool collapse = false)
    {
        PostingFormatOption formatOption(optionFlag);
        if (!formatOption.HasPositionList()) {
            return;
        }

        std::shared_ptr<Int32Encoder> posListEncoder;
        std::shared_ptr<Int8Encoder> posPayloadEncoder;

        PostingDecoderImpl decoder(formatOption);
        std::shared_ptr<TermMeta> termMeta(new TermMeta(1, posNum));
        decoder._termMeta = termMeta.get();

        size_t retLen = posNum;
        Sequence s;
        MockInt32Encoder* encoder = new MockInt32Encoder();
        EXPECT_CALL(*encoder, Decode(_, (size_t)posNum, _))
            .InSequence(s)
            .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

        decoder._positionEncoder = encoder;
        posListEncoder.reset(encoder);

        if (formatOption.HasPositionPayload()) {
            if (collapse) {
                retLen++;
            }
            MockInt8Encoder* encoder = new MockInt8Encoder();
            EXPECT_CALL(*encoder, Decode(_, (size_t)posNum, _))
                .InSequence(s)
                .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

            decoder._posPayloadEncoder = encoder;
            posPayloadEncoder.reset(encoder);
        }

        if (collapse) {
            ASSERT_THROW(decoder.DecodeDocList(nullptr, nullptr, nullptr, nullptr, posNum),
                         util::IndexCollapsedException);
        } else {
            decoder.DecodeDocList(nullptr, nullptr, nullptr, nullptr, posNum);
            ASSERT_EQ((df_t)posNum, decoder._decodedPosCount);
        }
    }

    void InnerTestDecodeDocList(optionflag_t optionFlag, uint32_t docNum, bool collapse = false)
    {
        std::shared_ptr<Int32Encoder> docIdEncoder;
        std::shared_ptr<Int32Encoder> tfListEncoder;
        std::shared_ptr<Int16Encoder> docPayloadEncoder;
        std::shared_ptr<Int8Encoder> fieldMapEncoder;

        std::shared_ptr<TermMeta> termMeta(new TermMeta(docNum, 1));
        PostingFormatOption formatOption(optionFlag);
        PostingDecoderImpl decoder(formatOption);
        decoder._termMeta = termMeta.get();

        size_t retLen = docNum;
        Sequence s;
        MockInt32Encoder* encoder = new MockInt32Encoder();
        EXPECT_CALL(*encoder, Decode(_, (size_t)docNum, _))
            .InSequence(s)
            .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

        decoder._docIdEncoder = encoder;
        docIdEncoder.reset(encoder);

        if (formatOption.HasTfList()) {
            if (collapse) {
                retLen++;
            }
            MockInt32Encoder* encoder = new MockInt32Encoder();
            EXPECT_CALL(*encoder, Decode(_, (size_t)docNum, _))
                .InSequence(s)
                .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

            decoder._tfListEncoder = encoder;
            tfListEncoder.reset(encoder);
        }

        if (formatOption.HasDocPayload()) {
            if (collapse) {
                retLen++;
            }
            MockInt16Encoder* encoder = new MockInt16Encoder();
            EXPECT_CALL(*encoder, Decode(_, (size_t)docNum, _))
                .InSequence(s)
                .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

            decoder._docPayloadEncoder = encoder;
            docPayloadEncoder.reset(encoder);
        }

        if (formatOption.HasFieldMap()) {
            if (collapse) {
                retLen++;
            }
            MockInt8Encoder* encoder = new MockInt8Encoder();
            EXPECT_CALL(*encoder, Decode(_, (size_t)docNum, _))
                .InSequence(s)
                .WillOnce(Return(std::make_pair(Status::OK(), retLen)));

            decoder._fieldMapEncoder = encoder;
            fieldMapEncoder.reset(encoder);
        }

        if (collapse) {
            ASSERT_THROW(decoder.DecodeDocList(nullptr, nullptr, nullptr, nullptr, docNum),
                         util::IndexCollapsedException);
        } else {
            decoder.DecodeDocList(nullptr, nullptr, nullptr, nullptr, docNum);
            ASSERT_EQ((df_t)docNum, decoder._decodedDocCount);
        }
    }
};

TEST_F(PostingDecoderImplTest, testInit)
{
    InnerTest(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTest(OPTION_FLAG_ALL, 10);
    InnerTest(NO_POSITION_LIST, 10);
    InnerTest(NO_TERM_FREQUENCY, 10);
    InnerTest(OPTION_FLAG_ALL & ~of_doc_payload, 10);
    InnerTest(OPTION_FLAG_ALL & ~of_position_payload, 10);
}

TEST_F(PostingDecoderImplTest, testInitForDictInlineCompress)
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
    dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(compressValue, false, true);
    decoder.Init(&termMeta, dictInlineValue, false, true);
    ASSERT_EQ(formatter.GetDocFreq(), termMeta.GetDocFreq());
    ASSERT_EQ(formatter.GetTermPayload(), termMeta.GetPayload());
    ASSERT_EQ(formatter.GetTermFreq(), termMeta.GetTotalTermFreq());
    ASSERT_TRUE(decoder._inlineDictValue != INVALID_DICT_VALUE);
}

TEST_F(PostingDecoderImplTest, testDecodeDictInlineDocList)
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

    bool isDocList = false;
    bool dfFirst = true;
    dictvalue_t dictInlineValue = 0;
    dictInlineValue = ShortListOptimizeUtil::CreateDictInlineValue(compressValue, isDocList, dfFirst);
    decoder.Init(&termMeta, dictInlineValue, isDocList, dfFirst);

    docid_t docId;
    docpayload_t docPayload;
    tf_t termFreq;
    fieldmap_t fieldMap;

    uint32_t decodedDocNum = decoder.DecodeDocList(&docId, &termFreq, &docPayload, &fieldMap, 128);
    ASSERT_EQ((uint32_t)1, decodedDocNum);
    ASSERT_EQ((tf_t)4, termFreq);
    ASSERT_EQ((docpayload_t)3, docPayload);
    ASSERT_EQ((fieldmap_t)5, fieldMap);
}

TEST_F(PostingDecoderImplTest, testMissPositionBitmap)
{
    ASSERT_THROW(InnerTest(OPTION_FLAG_ALL | of_tf_bitmap, 10, false), util::InconsistentStateException);
}

TEST_F(PostingDecoderImplTest, testDecodeDocList)
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(NO_TERM_FREQUENCY, 10);
    InnerTestDecodeDocList(OPTION_FLAG_ALL & ~of_doc_payload, 10);
}

TEST_F(PostingDecoderImplTest, testDocListCollapsed)
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10, false);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_term_frequency, 10, false);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_term_frequency & ~of_doc_payload, 10, false);
}

TEST_F(PostingDecoderImplTest, testDecodePosList)
{
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10);
    InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL & ~of_position_payload, 10);
}

TEST_F(PostingDecoderImplTest, testPosListCollapsed) { InnerTestDecodeDocList(EXPACK_OPTION_FLAG_ALL, 10, false); }

} // namespace indexlib::index
