#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/multi_section_meta.h"
#include "indexlib/common/field_format/section_attribute/section_meta.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class MultiSectionMetaTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestInit()
    {
        const uint32_t SECTION_COUNT = 100;
        const uint32_t BYTES_PER_SECTION = sizeof(section_fid_t) + sizeof(section_len_t) + 
                                   sizeof(section_weight_t);

        {
            uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
            vector<SectionMeta> answer;
            PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, true, true);

            MultiSectionMeta multiMeta;
            multiMeta.Init(metaBuf, true, true);        
            CheckMultiSectionMeta(answer, multiMeta, true, true);
        }

        {
            uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
            vector<SectionMeta> answer;
            PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, false, true);

            MultiSectionMeta multiMeta;
            multiMeta.Init(metaBuf, false, true);        
            CheckMultiSectionMeta(answer, multiMeta, false, true);
        }

        {
            uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
            vector<SectionMeta> answer;
            PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, true, false);
            MultiSectionMeta multiMeta;
            multiMeta.Init(metaBuf, true, false);        
            CheckMultiSectionMeta(answer, multiMeta, true, false);
        }

        {
            uint8_t metaBuf[sizeof(section_len_t) + SECTION_COUNT * BYTES_PER_SECTION];
            vector<SectionMeta> answer;
            PrepareMetaBuf(metaBuf, SECTION_COUNT, answer, false, false);
            MultiSectionMeta multiMeta;
            multiMeta.Init(metaBuf, false, false);        
            CheckMultiSectionMeta(answer, multiMeta, false, false);
        }
    }

private:
    void PrepareMetaBuf(uint8_t* metaBuf, uint32_t secCount,
                        vector<SectionMeta>& answer, bool hasFieldId, bool hasWeight)
    {
        const uint32_t FIELD_COUNT = 6;
        const uint32_t SECTION_COUNT_PER_FIELD = secCount / FIELD_COUNT;
        
        uint8_t* cursor = metaBuf;
        *((section_len_t*)cursor) = secCount;
        cursor += sizeof(section_len_t);
        section_len_t* secLens = (section_len_t*)cursor;
        cursor += sizeof(section_len_t) * secCount;

        section_fid_t* fids = NULL;
        if (hasFieldId)
        {
            fids = (section_fid_t*)cursor;
            cursor += sizeof(section_fid_t) * secCount;
        }

        section_weight_t* secWeights = NULL;
        if (hasWeight)
        {
            secWeights = (section_weight_t*)cursor;
        }

        for (uint32_t i = 0; i < secCount; ++i)
        {
            SectionMeta section;
            section.fieldId = i / SECTION_COUNT_PER_FIELD;
            section.length = rand() % 2000 + 1;
            section.weight = rand() % 500;
            answer.push_back(section);

            if (fids != NULL)
            {
                fids[i] = section.fieldId;
            }
            secLens[i] = section.length;
            if (hasWeight)
            {
                secWeights[i] = section.weight;
            }
        }
    }

    void CheckMultiSectionMeta(const vector<SectionMeta>& answer,
                               const MultiSectionMeta& multiMeta,
                               bool hasFieldId,
                               bool hasSectionWeight)
    {
        INDEXLIB_TEST_EQUAL(answer.size(), multiMeta.GetSectionCount());

        for (size_t i = 0; i < answer.size(); ++i)
        {
            if (hasFieldId)
            {
                INDEXLIB_TEST_EQUAL(answer[i].fieldId,
                        multiMeta.GetFieldId(i));
            }
            else
            {
                INDEXLIB_TEST_EQUAL((section_fid_t)0,
                        multiMeta.GetFieldId(i));
            }

            if (hasSectionWeight)
            {
                INDEXLIB_TEST_EQUAL(answer[i].weight,
                        multiMeta.GetSectionWeight(i));
            }
            else
            {
                INDEXLIB_TEST_EQUAL((section_weight_t)0,
                        multiMeta.GetSectionWeight(i)); 
            }

            INDEXLIB_TEST_EQUAL(answer[i].length,
                    multiMeta.GetSectionLen(i));
        }
    }
};

INDEXLIB_UNIT_TEST_CASE(MultiSectionMetaTest, TestInit);

IE_NAMESPACE_END(index);
