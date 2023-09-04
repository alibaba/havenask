#include "indexlib/index/inverted_index/format/DictInlinePostingDecoder.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class DictInlinePostingDecoderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

TEST_F(DictInlinePostingDecoderTest, testDecodeDocBuffer)
{
    // termpayload, docid, docpayload, termfreq, fieldmap
    uint32_t values[] = {1, 2, 3, 4, 5};

    PostingFormatOption formatOption(EXPACK_OPTION_FLAG_ALL);

    std::vector<uint32_t> vec(values, values + sizeof(values) / sizeof(uint32_t));
    DictInlineFormatter formatter(formatOption, vec);
    uint64_t dictValue;
    formatter.GetDictInlineValue(dictValue);

    DictInlinePostingDecoder decoder(formatOption, (dictvalue_t)dictValue, false, true);

    docid_t firstDocId;
    docid_t lastDocId;
    ttf_t currentTTF;
    docid_t docBuffer;
    {
        // test startDocid > lastDocid
        ASSERT_FALSE(decoder.DecodeDocBuffer(3, &docBuffer, firstDocId, lastDocId, currentTTF));
    }
    {
        // test startDocid <= lastDocid
        ASSERT_TRUE(decoder.DecodeDocBuffer(2, &docBuffer, firstDocId, lastDocId, currentTTF));
        ASSERT_EQ((docid_t)2, firstDocId);
        ASSERT_EQ((docid_t)2, lastDocId);
        ASSERT_EQ((docid_t)2, docBuffer);
        ASSERT_EQ((ttf_t)4, currentTTF);

        tf_t tfBuffer;
        decoder.DecodeCurrentTFBuffer(&tfBuffer);
        ASSERT_EQ((tf_t)4, tfBuffer);

        docpayload_t docPayload;
        decoder.DecodeCurrentDocPayloadBuffer(&docPayload);
        ASSERT_EQ((docpayload_t)3, docPayload);

        fieldmap_t fieldMap;
        decoder.DecodeCurrentFieldMapBuffer(&fieldMap);
        ASSERT_EQ((fieldmap_t)5, fieldMap);
    }
}

} // namespace indexlib::index
