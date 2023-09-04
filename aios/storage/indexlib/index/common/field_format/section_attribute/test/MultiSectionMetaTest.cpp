#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"

#include "indexlib/index/common/field_format/section_attribute/SectionMeta.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class MultiSectionMetaTest : public TESTBASE
{
public:
    MultiSectionMetaTest() = default;
    ~MultiSectionMetaTest() = default;

    void setUp() override {};
    void tearDown() override {};

private:
    void PrepareMetaBuf(uint8_t* metaBuf, uint32_t secCount, std::vector<SectionMeta>& answer, bool hasFieldId,
                        bool hasWeight)
    {
        const uint32_t FIELD_COUNT = 6;
        const uint32_t SECTION_COUNT_PER_FIELD = secCount / FIELD_COUNT;

        uint8_t* cursor = metaBuf;
        *((section_len_t*)cursor) = secCount;
        cursor += sizeof(section_len_t);
        section_len_t* secLens = (section_len_t*)cursor;
        cursor += sizeof(section_len_t) * secCount;

        section_fid_t* fids = NULL;
        if (hasFieldId) {
            fids = (section_fid_t*)cursor;
            cursor += sizeof(section_fid_t) * secCount;
        }

        section_weight_t* secWeights = NULL;
        if (hasWeight) {
            secWeights = (section_weight_t*)cursor;
        }

        for (uint32_t i = 0; i < secCount; ++i) {
            SectionMeta section;
            section.fieldId = i / SECTION_COUNT_PER_FIELD;
            section.length = rand() % 2000 + 1;
            section.weight = rand() % 500;
            answer.push_back(section);

            if (fids != NULL) {
                fids[i] = section.fieldId;
            }
            secLens[i] = section.length;
            if (hasWeight) {
                secWeights[i] = section.weight;
            }
        }
    }

    void CheckMultiSectionMeta(const std::vector<SectionMeta>& answer, const MultiSectionMeta& multiMeta,
                               bool hasFieldId, bool hasSectionWeight)
    {
        ASSERT_EQ(answer.size(), multiMeta.GetSectionCount());

        for (size_t i = 0; i < answer.size(); ++i) {
            if (hasFieldId) {
                ASSERT_EQ(answer[i].fieldId, multiMeta.GetFieldId(i));
            } else {
                ASSERT_EQ((section_fid_t)0, multiMeta.GetFieldId(i));
            }

            if (hasSectionWeight) {
                ASSERT_EQ(answer[i].weight, multiMeta.GetSectionWeight(i));
            } else {
                ASSERT_EQ((section_weight_t)0, multiMeta.GetSectionWeight(i));
            }

            ASSERT_EQ(answer[i].length, multiMeta.GetSectionLen(i));
        }
    }
};

TEST_F(MultiSectionMetaTest, TestInit)
{
    const uint32_t SECTION_COUNT = 100;
    const uint32_t BYTES_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + sizeof(section_weight_t);

    {
        uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
        std::vector<SectionMeta> answer;
        PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, true, true);

        MultiSectionMeta multiMeta;
        multiMeta.Init(metaBuf, true, true);
        CheckMultiSectionMeta(answer, multiMeta, true, true);
    }

    {
        uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
        std::vector<SectionMeta> answer;
        PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, false, true);

        MultiSectionMeta multiMeta;
        multiMeta.Init(metaBuf, false, true);
        CheckMultiSectionMeta(answer, multiMeta, false, true);
    }

    {
        uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
        std::vector<SectionMeta> answer;
        PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, true, false);
        MultiSectionMeta multiMeta;
        multiMeta.Init(metaBuf, true, false);
        CheckMultiSectionMeta(answer, multiMeta, true, false);
    }

    {
        uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
        std::vector<SectionMeta> answer;
        PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, false, false);
        MultiSectionMeta multiMeta;
        multiMeta.Init(metaBuf, false, false);
        CheckMultiSectionMeta(answer, multiMeta, false, false);
    }
}
} // namespace indexlib::index
