#include "indexlib/common/field_format/section_attribute/section_attribute_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "indexlib/index_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::util;
;

namespace indexlib { namespace common {

class SectionAttributeEncoderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestEncodeOneSection() { TestEncode(1); }

    void TestEncodeSeveralSections() { TestEncode(7); }

    void TestEncodeManySections()
    {
        TestEncode(21);
        TestEncode(60);
    }

    void TestEncodeWithShortEncodeBuffer()
    {
        SectionAttributeEncoder encoder;

        vector<SectionMeta> answer;
        section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
        section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
        section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
        CreateSectionData(30, answer, lengths, fids, weights);

        const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
        uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
        ASSERT_THROW(encoder.Encode(lengths, fids, weights, 30, encodedBuf, 30), IndexCollapsedException);
    }

    void TestDecodeWithAllWeightZero()
    {
        SectionAttributeEncoder encoder;
        section_len_t lengths[] = {1};
        section_fid_t fids[] = {1};
        section_weight_t weights[] = {0};
        uint8_t* encodedBuf = new uint8_t[4097];
        mprotect(encodedBuf + 4096, 1, PROT_NONE);

        uint32_t encodedLen = encoder.Encode(lengths, fids, weights, 1, encodedBuf + 4087, 9);
        ASSERT_EQ((uint32_t)9, encodedLen);

        uint8_t decodedBuf[100];
        encoder.Decode(encodedBuf + 4087, encodedLen, decodedBuf, 100);
        delete[] encodedBuf;
    }

    void TestDecodeWithShortDecodeBuffer()
    {
        SectionAttributeEncoder encoder;

        vector<SectionMeta> answer;
        uint32_t sectionCount = 30;
        section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
        section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
        section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
        CreateSectionData(30, answer, lengths, fids, weights);

        const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
        uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
        uint32_t encodedLen = encoder.Encode(lengths, fids, weights, sectionCount, encodedBuf, MAX_ENCODED_BUF_LEN);

        const uint32_t SIZE_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + sizeof(section_weight_t);
        const uint32_t MAX_DECODED_BUF_LEN = SIZE_PER_SECTION * MAX_SECTION_COUNT_PER_DOC;

        uint8_t decodedBuf[MAX_DECODED_BUF_LEN];
        ASSERT_THROW(encoder.Decode(encodedBuf, encodedLen, decodedBuf, 30), BufferOverflowException);
    }

private:
    void CreateSectionData(uint32_t sectionCount, vector<SectionMeta>& answer, section_len_t* lengths,
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

    void CheckSections(const MultiSectionMeta& multiMeta, uint32_t sectionCount, vector<SectionMeta>& answer)
    {
        INDEXLIB_TEST_EQUAL(answer.size(), (size_t)sectionCount);
        INDEXLIB_TEST_EQUAL(sectionCount, multiMeta.GetSectionCount());

        for (size_t i = 0; i < answer.size(); ++i) {
            INDEXLIB_TEST_EQUAL(answer[i].fieldId, multiMeta.GetFieldId(i));
            INDEXLIB_TEST_EQUAL(answer[i].length, multiMeta.GetSectionLen(i));
            assert(answer[i].weight == multiMeta.GetSectionWeight(i));
            INDEXLIB_TEST_EQUAL(answer[i].weight, multiMeta.GetSectionWeight(i));
        }
    }

    void TestEncode(uint32_t sectionCount)
    {
        SectionAttributeEncoder encoder;

        vector<SectionMeta> answer;
        section_len_t lengths[MAX_SECTION_COUNT_PER_DOC];
        section_fid_t fids[MAX_SECTION_COUNT_PER_DOC];
        section_weight_t weights[MAX_SECTION_COUNT_PER_DOC];
        CreateSectionData(sectionCount, answer, lengths, fids, weights);

        const uint32_t MAX_ENCODED_BUF_LEN = 3 * sizeof(uint32_t) + sizeof(SectionMeta) * MAX_SECTION_COUNT_PER_DOC;
        uint8_t encodedBuf[MAX_ENCODED_BUF_LEN];
        uint32_t encodedLen = encoder.Encode(lengths, fids, weights, sectionCount, encodedBuf, MAX_ENCODED_BUF_LEN);

        const uint32_t SIZE_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + sizeof(section_weight_t);
        const uint32_t MAX_DECODED_BUF_LEN = SIZE_PER_SECTION * MAX_SECTION_COUNT_PER_DOC;

        uint8_t decodedBuf[MAX_DECODED_BUF_LEN];
        char* base = (char*)decodedBuf;
        encoder.Decode(encodedBuf, encodedLen, decodedBuf, MAX_DECODED_BUF_LEN);
        uint32_t decodedSectionNum = ((section_len_t*)base)[0];
        MultiSectionMeta meta;
        meta.Init(decodedBuf, true, true);
        CheckSections(meta, decodedSectionNum, answer);
    }
};

INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestEncodeOneSection);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestEncodeSeveralSections);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestEncodeManySections);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestEncodeWithShortEncodeBuffer);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestDecodeWithShortDecodeBuffer);
INDEXLIB_UNIT_TEST_CASE(SectionAttributeEncoderTest, TestDecodeWithAllWeightZero);
}} // namespace indexlib::common
