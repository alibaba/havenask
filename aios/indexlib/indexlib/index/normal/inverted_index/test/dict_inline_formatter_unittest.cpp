#include "indexlib/index/normal/inverted_index/test/dict_inline_formatter_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DictInlineFormatterTest);

DictInlineFormatterTest::DictInlineFormatterTest()
{
}

DictInlineFormatterTest::~DictInlineFormatterTest()
{
}

void DictInlineFormatterTest::CaseSetUp()
{
}

void DictInlineFormatterTest::CaseTearDown()
{
}

void DictInlineFormatterTest::TestFromVector()
{
    //data[] = {termpayload, docid, docpayload, termfreq, fieldmap}
    {
        //normal case
        uint32_t data[] = {1, 2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(EXPACK_OPTION_FLAG_ALL);
        DictInlineFormatter formatter(formatOption);

        formatter.FromVector((uint32_t*)vec.data(), vec.size());
        CheckFormatterElements(data, formatter);
    }

    {
        //normal case without termpayload
        uint32_t data[] = {2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_term_payload);
        DictInlineFormatter formatter(formatOption);
        uint32_t expectData[] = {0, 2, 3, 4, 5};
        formatter.FromVector((uint32_t*)vec.data(), vec.size());
        CheckFormatterElements(expectData, formatter);
    }

    {
        //normal case without fieldmap
        uint32_t data[] = {1, 2, 3, 4};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_fieldmap);
        DictInlineFormatter formatter(formatOption);

        formatter.FromVector((uint32_t*)vec.data(), vec.size());
        uint32_t expectData[] = {1, 2, 3, 4, 0};
        CheckFormatterElements(expectData, formatter);
    }

    {
        //normal case without docpayload
        uint32_t data[] = {1, 2, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_doc_payload);
        DictInlineFormatter formatter(formatOption);

        formatter.FromVector((uint32_t*)vec.data(), vec.size());
        uint32_t expectData[] = {1, 2, (uint32_t)INVALID_DOC_PAYLOAD, 4, 5};
        CheckFormatterElements(expectData, formatter);
    }

    {
        //normal case without tf
        uint32_t data[] = {1, 2, 3, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_term_frequency);
        DictInlineFormatter formatter(formatOption);

        formatter.FromVector((uint32_t*)vec.data(), vec.size());
        uint32_t expectData[] = {1, 2, 3, 1, 5};
        CheckFormatterElements(expectData, formatter);
    }

    {
        //exception: more data than needed
        uint32_t data[] = {1, 2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_term_frequency);
        DictInlineFormatter formatter(formatOption);

        ASSERT_ANY_THROW(formatter.FromVector((uint32_t*)vec.data(), vec.size()));
    }

    {
        //exception: less data than needed
        uint32_t data[] = {1, 2, 3, 4};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL);
        DictInlineFormatter formatter(formatOption);

        ASSERT_ANY_THROW(formatter.FromVector((uint32_t*)vec.data(), vec.size()));
    }
}

void DictInlineFormatterTest::TestToVector()
{
    //data[] = {termpayload, docid, docpayload, termfreq, fieldmap}
    {
        //normal case
        uint32_t data[] = {1, 2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(EXPACK_OPTION_FLAG_ALL);
        DictInlineFormatter formatter(formatOption, vec);

        vector<uint32_t> targetVec;
        formatter.ToVector(targetVec);
        CheckVectorEqual(targetVec, vec);
    }

    {
        //normal case without termpayload
        uint32_t data[] = {2, 3, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_term_payload);
        DictInlineFormatter formatter(formatOption, vec);

        vector<uint32_t> targetVec;
        formatter.ToVector(targetVec);
        CheckVectorEqual(targetVec, vec);
    }

    {
        //normal case without fieldmap
        uint32_t data[] = {1, 2, 3, 4};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_fieldmap);
        DictInlineFormatter formatter(formatOption, vec);

        vector<uint32_t> targetVec;
        formatter.ToVector(targetVec);
        CheckVectorEqual(targetVec, vec);
    }

    {
        //normal case without docpayload
        uint32_t data[] = {1, 2, 4, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));
        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_doc_payload);
        DictInlineFormatter formatter(formatOption, vec);

        vector<uint32_t> targetVec;
        formatter.ToVector(targetVec);
        CheckVectorEqual(targetVec, vec);
    }

    {
        //normal case without tf
        uint32_t data[] = {1, 2, 3, 5};
        vector<uint32_t> vec(data, data + sizeof(data) / sizeof(uint32_t));

        PostingFormatOption formatOption(
                EXPACK_OPTION_FLAG_ALL & ~of_term_frequency);
        DictInlineFormatter formatter(formatOption, vec);

        vector<uint32_t> targetVec;
        formatter.ToVector(targetVec);
        CheckVectorEqual(targetVec, vec);
    }
}

void DictInlineFormatterTest::CheckVectorEqual(
        const vector<uint32_t>& leftVector,
        const vector<uint32_t>& rightVector)
{
    ASSERT_EQ(leftVector.size(), rightVector.size());
    for (size_t i = 0; i < leftVector.size(); i++)
    {
        ASSERT_EQ(leftVector[i], rightVector[i]);
    }
}

void DictInlineFormatterTest::CheckFormatterElements(
        uint32_t expectData[], DictInlineFormatter& formatter)
{
    ASSERT_EQ((termpayload_t)expectData[0], formatter.GetTermPayload());
    ASSERT_EQ((docid_t)expectData[1], formatter.GetDocId());
    ASSERT_EQ((docpayload_t)expectData[2], formatter.GetDocPayload());    
    ASSERT_EQ((tf_t)expectData[3], formatter.GetTermFreq());
    ASSERT_EQ((fieldmap_t)expectData[4], formatter.GetFieldMap());
}

void DictInlineFormatterTest::TestCalculateDictInlineItemCount()
{
    optionflag_t optionFlag = EXPACK_OPTION_FLAG_ALL;
    {
        PostingFormatOption formatOption(optionFlag);
        ASSERT_EQ((uint8_t)5, DictInlineFormatter::CalculateDictInlineItemCount(
                        formatOption));
    }

    {
        optionFlag = optionFlag & ~of_term_payload;
        PostingFormatOption formatOption(optionFlag);
        ASSERT_EQ((uint8_t)4, DictInlineFormatter::CalculateDictInlineItemCount(
                        formatOption));
    }

    {
        optionFlag = optionFlag & ~of_term_frequency;
        PostingFormatOption formatOption(optionFlag);
        ASSERT_EQ((uint8_t)3, DictInlineFormatter::CalculateDictInlineItemCount(
                        formatOption));
    }

    {
        optionFlag = optionFlag & ~of_doc_payload;
        PostingFormatOption formatOption(optionFlag);
        ASSERT_EQ((uint8_t)2, DictInlineFormatter::CalculateDictInlineItemCount(
                        formatOption));
    }

    {
        optionFlag = optionFlag & ~of_fieldmap;
        PostingFormatOption formatOption(optionFlag);
        ASSERT_EQ((uint8_t)1, DictInlineFormatter::CalculateDictInlineItemCount(
                        formatOption));
    }
}

IE_NAMESPACE_END(index);

