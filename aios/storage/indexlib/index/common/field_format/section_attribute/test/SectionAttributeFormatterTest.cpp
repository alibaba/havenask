#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"

#include "unittest/unittest.h"

namespace indexlib::index {

class SectionAttributeFormatterTest : public TESTBASE
{
public:
    SectionAttributeFormatterTest() = default;
    ~SectionAttributeFormatterTest() = default;

private:
    std::shared_ptr<SectionAttributeFormatter> CreateFormatter(bool hasFieldId, bool hasWeight);

    void CheckBuffer(uint8_t* buffer, uint32_t sectionCount, const section_len_t* lenBuffer,
                     const section_fid_t* fidBuffer, const section_weight_t* weightBuffer);
};

std::shared_ptr<SectionAttributeFormatter> SectionAttributeFormatterTest::CreateFormatter(bool hasFieldId,
                                                                                          bool hasWeight)
{
    auto sectionAttrConfig = std::make_shared<indexlibv2::config::SectionAttributeConfig>("", hasWeight, hasFieldId);
    return std::make_shared<SectionAttributeFormatter>(sectionAttrConfig);
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

TEST_F(SectionAttributeFormatterTest, TestSimpleProcess)
{
    section_len_t lengths[] = {1, 2, 8};
    section_fid_t fids[] = {3, 4, 1};
    section_fid_t decodeFids[] = {3, 7, 8}; // 3, 3 + 4, 3 + 4 + 1
    section_weight_t weights[] = {5, 6, 8};

    {
        auto formatter = CreateFormatter(true, true);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 3);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 3, lengths, decodeFids, weights);
    }

    {
        auto formatter = CreateFormatter(false, true);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 3);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 3, lengths, NULL, weights);
    }

    {
        auto formatter = CreateFormatter(true, false);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 3);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 3, lengths, decodeFids, NULL);
    }

    {
        auto formatter = CreateFormatter(false, false);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 3);

        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 3, lengths, NULL, NULL);
    }

    // below : empty section encode & decode
    {
        auto formatter = CreateFormatter(true, true);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 0);
        ASSERT_TRUE(status.IsOK());

        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        auto formatter = CreateFormatter(false, true);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        auto formatter = CreateFormatter(true, false);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }

    {
        auto formatter = CreateFormatter(false, false);
        auto [status, result] = formatter->Encode(lengths, fids, weights, 0);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(result.size() > (size_t)0);
        ASSERT_EQ(result.size() % 4, (size_t)0);

        uint8_t buf[100];
        status = formatter->Decode(result, buf, 100);
        ASSERT_TRUE(status.IsOK());
        CheckBuffer(buf, 0, lengths, decodeFids, weights);
    }
}

TEST_F(SectionAttributeFormatterTest, TestUnpackBuffer)
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

} // namespace indexlib::index
