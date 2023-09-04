#include "indexlib/index/common/field_format/section_attribute/SectionAttributeEncoder.h"

#include <malloc.h>
#include <vector>

#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class SectionAttributeEncoderTest : public TESTBASE
{
public:
    SectionAttributeEncoderTest() = default;
    ~SectionAttributeEncoderTest() = default;
    void setUp() override {}

    void tearDown() override {}

private:
    void CreateSectionData(uint32_t sectionCount, std::vector<SectionMeta>& answer, section_len_t* lengths,
                           section_fid_t* fids, section_weight_t* weights)
    {
        const uint32_t FIELD_COUNT = 6 > sectionCount ? sectionCount : 6;
        uint32_t sectionCountPerField = sectionCount / FIELD_COUNT;
        fieldid_t lastFieldId = 0;

        for (uint32_t i = 0; i < sectionCount; ++i) {
            SectionMeta section;
            section.fieldId = i / sectionCountPerField;
            if (section.fieldId >= FIELD_COUNT)
                section.fieldId = FIELD_COUNT - 1;
            section.length = rand() % 2000 + 1;

            if (i % 5 == 1) {
                section.weight = rand() % 500;
            } else {
                section.weight = 0;
            }
            answer.push_back(section);

            lengths[i] = section.length;
            fids[i] = section.fieldId - lastFieldId;
            weights[i] = section.weight;
            lastFieldId = section.fieldId;
        }
    }

    void CheckSections(const MultiSectionMeta& multiMeta, uint32_t sectionCount, std::vector<SectionMeta>& answer)
    {
        ASSERT_EQ(answer.size(), (size_t)sectionCount);
        ASSERT_EQ(sectionCount, multiMeta.GetSectionCount());

        for (size_t i = 0; i < answer.size(); ++i) {
            ASSERT_EQ(answer[i].fieldId, multiMeta.GetFieldId(i));
            ASSERT_EQ(answer[i].length, multiMeta.GetSectionLen(i));
            ASSERT_EQ(answer[i].weight, multiMeta.GetSectionWeight(i));
            ASSERT_EQ(answer[i].weight, multiMeta.GetSectionWeight(i));
        }
    }

    void TestEncode(uint32_t sectionCount)
    {
        SectionAttributeEncoder encoder;

        std::vector<SectionMeta> answer;
        section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
        section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
        section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
        CreateSectionData(sectionCount, answer, lengths, fids, weights);

        const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
        uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
        auto [st, encodedLen] = encoder.Encode(lengths, fids, weights, sectionCount, encodedBuf, MAX_ENCODED_BUF_LEN);
        ASSERT_TRUE(st.IsOK());

        const uint32_t SIZE_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + sizeof(section_weight_t);
        const uint32_t MAX_DECODED_BUF_LEN = SIZE_PER_SECTION * MAX_SECTION_COUNT_PER_DOC;

        uint8_t decodedBuf[MAX_DECODED_BUF_LEN];
        char* base = (char*)decodedBuf;
        st = encoder.Decode(encodedBuf, encodedLen, decodedBuf, MAX_DECODED_BUF_LEN);
        ASSERT_TRUE(st.IsOK());
        uint32_t decodedSectionNum = ((section_len_t*)base)[0];
        MultiSectionMeta meta;
        meta.Init(decodedBuf, true, true);
        CheckSections(meta, decodedSectionNum, answer);
    }
};

TEST_F(SectionAttributeEncoderTest, TestEncodeOneSection) { TestEncode(1); }
TEST_F(SectionAttributeEncoderTest, TestEncodeSeveralSections) { TestEncode(7); }
TEST_F(SectionAttributeEncoderTest, TestEncodeManySections)
{
    TestEncode(21);
    TestEncode(60);
}
TEST_F(SectionAttributeEncoderTest, TestEncodeWithShortEncodeBuffer)
{
    SectionAttributeEncoder encoder;

    std::vector<SectionMeta> answer;
    section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
    section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
    section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
    CreateSectionData(30, answer, lengths, fids, weights);

    const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
    uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
    auto [st, _] = encoder.Encode(lengths, fids, weights, 30, encodedBuf, 30);
    ASSERT_TRUE(st.IsCorruption());
}

TEST_F(SectionAttributeEncoderTest, TestDecodeWithShortDecodeBuffer)
{
    SectionAttributeEncoder encoder;

    std::vector<SectionMeta> answer;
    uint32_t sectionCount = 30;
    section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
    section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
    section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
    CreateSectionData(30, answer, lengths, fids, weights);

    const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
    uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
    auto [st, encodedLen] = encoder.Encode(lengths, fids, weights, sectionCount, encodedBuf, MAX_ENCODED_BUF_LEN);
    ASSERT_TRUE(st.IsOK());

    const uint32_t SIZE_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + sizeof(section_weight_t);
    const uint32_t MAX_DECODED_BUF_LEN = SIZE_PER_SECTION * MAX_SECTION_COUNT_PER_DOC;

    uint8_t decodedBuf[MAX_DECODED_BUF_LEN];
    st = encoder.Decode(encodedBuf, encodedLen, decodedBuf, 30);
    ASSERT_TRUE(st.IsCorruption());
}
TEST_F(SectionAttributeEncoderTest, TestDecodeWithAllWeightZero)
{
    SectionAttributeEncoder encoder;
    section_len_t lengths[] = {1};
    section_fid_t fids[] = {1};
    section_weight_t weights[] = {0};
    auto deleter = [](uint8_t* p) {
        ASSERT_EQ(0, (mprotect(p + 4096, 1, PROT_WRITE))) << strerror(errno);
        free(p);
    };
    std::unique_ptr<uint8_t, decltype(deleter)> encodedBuf((uint8_t*)memalign(getpagesize(), 8192), deleter);
    memset(encodedBuf.get(), 0, 8192);
    ASSERT_EQ(0, mprotect(encodedBuf.get() + 4096, 1, PROT_NONE)) << strerror(errno);

    auto [st, encodedLen] = encoder.Encode(lengths, fids, weights, 1, encodedBuf.get() + 4087, 9);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ((uint32_t)9, encodedLen);

    uint8_t decodedBuf[100];
    st = encoder.Decode(encodedBuf.get() + 4087, encodedLen, decodedBuf, 100);
    ASSERT_TRUE(st.IsOK());
}
} // namespace indexlib::index
