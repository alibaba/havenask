#include "indexlib/common/field_format/section_attribute/test/section_attribute_formatter_unittest.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, SectionAttributeFormatterTest);

SectionAttributeFormatterTest::SectionAttributeFormatterTest() {}

SectionAttributeFormatterTest::~SectionAttributeFormatterTest() {}

void SectionAttributeFormatterTest::TestSimpleProcess()
{
    section_len_t lengths[] = {1, 2, 8};
    section_fid_t fids[] = {3, 4, 1};
    section_fid_t decodeFids[] = {3, 7, 8}; // 3, 3 + 4, 3 + 4 + 1
    section_weight_t weights[] = {5, 6, 8};

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(true, true);
        string result = formatter->Encode(lengths, fids, weights, 3);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 3, lengths, decodeFids, weights);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(false, true);
        string result = formatter->Encode(lengths, fids, weights, 3);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 3, lengths, NULL, weights);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(true, false);
        string result = formatter->Encode(lengths, fids, weights, 3);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 3, lengths, decodeFids, NULL);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(false, false);
        string result = formatter->Encode(lengths, fids, weights, 3);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 3, lengths, NULL, NULL);
    }

    // below : empty section encode & decode
    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(true, true);
        string result = formatter->Encode(lengths, fids, weights, 0);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(false, true);
        string result = formatter->Encode(lengths, fids, weights, 0);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(true, false);
        string result = formatter->Encode(lengths, fids, weights, 0);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        SectionAttributeFormatterPtr formatter = CreateFormatter(false, false);
        string result = formatter->Encode(lengths, fids, weights, 0);

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        formatter->Decode(result, buf, 100);
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }
}

SectionAttributeFormatterPtr SectionAttributeFormatterTest::CreateFormatter(bool hasFieldId, bool hasWeight)
{
    SectionAttributeConfigPtr sectionAttrConfig(new SectionAttributeConfig("", hasWeight, hasFieldId));
    return SectionAttributeFormatterPtr(new SectionAttributeFormatter(sectionAttrConfig));
}

void SectionAttributeFormatterTest::CheckBuffer(uint8_t* buffer, uint32_t sectionCount, const section_len_t* lenBuffer,
                                                const section_fid_t* fidBuffer, const section_weight_t* weightBuffer)
{
    section_len_t* dLenBuffer = (section_len_t*)buffer;
    section_fid_t* dFidBuffer = NULL;
    section_weight_t* dWeightBuffer = NULL;

    uint32_t count = sizeof(section_len_t) * (sectionCount + 1);
    if (fidBuffer) {
        dFidBuffer = (section_fid_t*)(buffer + count);
        count += sizeof(section_fid_t) * sectionCount;
    }

    if (weightBuffer) {
        dWeightBuffer = (section_weight_t*)(buffer + count);
    }

    ASSERT_EQ(sectionCount, (uint32_t)*dLenBuffer);
    for (uint32_t i = 0; i < sectionCount; ++i) {
        ASSERT_EQ(lenBuffer[i], dLenBuffer[i + 1]);
        if (fidBuffer) {
            ASSERT_EQ(fidBuffer[i], dFidBuffer[i]);
        }

        if (weightBuffer) {
            ASSERT_EQ(weightBuffer[i], dWeightBuffer[i]);
        }
    }
}

void SectionAttributeFormatterTest::TestUnpackBuffer()
{
    section_len_t sectionCount = 16;
    section_len_t* lenBuf = NULL;
    section_fid_t* fidBuf = NULL;
    section_weight_t* weightBuf = NULL;

    {
        ASSERT_EQ((uint32_t)sectionCount, SectionAttributeFormatter::UnpackBuffer((const uint8_t*)&sectionCount, true,
                                                                                  true, lenBuf, fidBuf, weightBuf));

        ASSERT_EQ((char*)&sectionCount + 2, (char*)lenBuf);
        ASSERT_EQ((char*)&sectionCount + 2 + 2 * sectionCount, (char*)fidBuf);
        ASSERT_EQ((char*)&sectionCount + 2 + (2 + 1) * sectionCount, (char*)weightBuf);
    }

    {
        ASSERT_EQ((uint32_t)sectionCount, SectionAttributeFormatter::UnpackBuffer((const uint8_t*)&sectionCount, false,
                                                                                  true, lenBuf, fidBuf, weightBuf));

        ASSERT_EQ((char*)&sectionCount + 2, (char*)lenBuf);
        ASSERT_TRUE(fidBuf == NULL);
        ASSERT_EQ((char*)&sectionCount + 2 + 2 * sectionCount, (char*)weightBuf);
    }

    {
        ASSERT_EQ((uint32_t)sectionCount, SectionAttributeFormatter::UnpackBuffer((const uint8_t*)&sectionCount, true,
                                                                                  false, lenBuf, fidBuf, weightBuf));

        ASSERT_EQ((char*)&sectionCount + 2, (char*)lenBuf);
        ASSERT_EQ((char*)&sectionCount + 2 + 2 * sectionCount, (char*)fidBuf);
        ASSERT_TRUE(weightBuf == NULL);
    }

    {
        ASSERT_EQ((uint32_t)sectionCount, SectionAttributeFormatter::UnpackBuffer((const uint8_t*)&sectionCount, false,
                                                                                  false, lenBuf, fidBuf, weightBuf));

        ASSERT_EQ((char*)&sectionCount + 2, (char*)lenBuf);
        ASSERT_TRUE(fidBuf == NULL);
        ASSERT_TRUE(weightBuf == NULL);
    }
}
}} // namespace indexlib::common
