#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/multi_value_attribute_segment_reader.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class MultiStringAttributeSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiStringAttributeSegmentReaderTest);
    MultiStringAttributeSegmentReaderTest()
    {
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<MultiChar>(true);
    }
    void CaseSetUp() override
    {
    }
    void CaseTearDown() override
    {
    }

    void TestCaseForOneDoc()
    {
        InnerTestCaseForOneDoc(false);
        InnerTestCaseForOneDoc(true);
    }

    void InnerTestCaseForOneDoc(bool uniqEncode)
    {
        TearDown();
        SetUp();
        mAttrConfig->SetUniqEncode(uniqEncode);
        string docStr = string("abc") + MULTI_VALUE_SEPARATOR + 
                        "aaa" + MULTI_VALUE_SEPARATOR + 
                        "test" + MULTI_VALUE_SEPARATOR + 
                        "";
        vector<string> fields;
        fields.push_back(docStr);

        MakeData(fields);

        vector<string> answer;
        vector<vector<string> > answers;
        answer.push_back("abc");
        answer.push_back("aaa");
        answer.push_back("test");
        answer.push_back("");
        answers.push_back(answer);

        CheckData(answers);
    }

    void TestCaseForRead()
    {
        InnerTestCaseForRead(false);
        InnerTestCaseForRead(true);
    }

    void InnerTestCaseForRead(bool uniqEncode)
    {
        TearDown();
        SetUp();
        mAttrConfig->SetUniqEncode(uniqEncode);
        uint32_t docCount = 40;
        vector<string> fields;
        vector<vector<string> > answers;
        GenerateFields(docCount, fields, answers);
        MakeData(fields);
        CheckData(answers);
    }

    void TestCaseForOffsetOver64K()
    {
        InnerTestCaseForOffsetOver64K(false);
        InnerTestCaseForOffsetOver64K(true);
    }

    void InnerTestCaseForOffsetOver64K(bool uniqEncode)
    {
        TearDown();
        SetUp();
        mAttrConfig->SetUniqEncode(uniqEncode);
        vector<string> fields;
        vector<vector<string> > answers;
        vector<string> answer;
        string oneDoc;
        uint32_t tokenNum = numeric_limits<uint16_t>::max() + 1000;
        for (size_t i = 0; i < tokenNum; ++i)
        {
            answer.push_back("a");
            oneDoc.append("a");
            if (i < tokenNum - 1)
            {
                oneDoc.append(string(1, MULTI_VALUE_SEPARATOR));
            }
        }
        fields.push_back(oneDoc);
        answers.push_back(answer);
        MakeData(fields);
        CheckData(answers);
    }

private:

    void GenerateFields(uint32_t docCount, 
                        vector<string>& fields, 
                        vector<vector<string> >& answers) 
    {
        answers.resize(docCount);
        for (uint32_t i = 0; i < docCount; ++i)
        {
            string str;
            for (uint32_t j = 0; j < i * 3 % 100; ++j)
            {
                if (j != 0)
                {
                    str += MULTI_VALUE_SEPARATOR;
                }
                string value;
                for (uint32_t k = 0; k < i * j%1000; ++k)
                {
                    value += ((i+j+k)%26 + 'a');
                }
                answers[i].push_back(value);
                str += value;
            }
            fields.push_back(str);
        }
    }


    void MakeData(const vector<string>& fields)
    {
        MultiStringAttributeWriter writer(mAttrConfig);
        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(mAttrConfig->GetFieldConfig()));
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);

        for (size_t i = 0; i < fields.size(); ++i)
        {
            autil::ConstString encodeValue = 
                convertor->Encode(autil::ConstString(fields[i]), &mPool);
            writer.AddField((docid_t)i, encodeValue);
        }

        file_system::DirectoryPtr attrDirectory = 
            GET_SEGMENT_DIRECTORY()->MakeDirectory(ATTRIBUTE_DIR_NAME);
        util::SimplePool dumpPool;
        writer.Dump(attrDirectory, &dumpPool);
    }

    void CheckData(const vector<vector<string> >& answers)
    {
        MultiValueAttributeSegmentReader<MultiChar> segReader(mAttrConfig);
        SegmentInfo segInfo;
        segInfo.docCount = answers.size();

        file_system::DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
        index_base::SegmentData segData = IndexTestUtil::CreateSegmentData(
                segDirectory, segInfo, 0, 0);
        segReader.Open(segData);
        for (size_t i = 0; i < answers.size(); ++i)
        {
            MultiString multiStr;
            INDEXLIB_TEST_TRUE(segReader.Read((docid_t)i, multiStr));
            INDEXLIB_TEST_EQUAL(answers[i].size(), multiStr.size());
            size_t lastOffset = 0;
            size_t dataLength = 0;
            for (size_t j = 0; j < answers[i].size(); ++j)
            {
                MultiChar multiChar = multiStr[j];
                const string& oneStr = answers[i][j];
                lastOffset = dataLength;
                dataLength += oneStr.size() +
                              VarNumAttributeFormatter::GetEncodedCountLength(
                                      oneStr.size());
                INDEXLIB_TEST_EQUAL(oneStr.size(), multiChar.size());
                for (size_t k = 0; k < oneStr.size(); ++k)
                {
                    INDEXLIB_TEST_EQUAL(oneStr[k], multiChar[k]);
                }
            }

            {
                CountedMultiString multiStr;
                INDEXLIB_TEST_TRUE(segReader.Read((docid_t)i, multiStr));
                INDEXLIB_TEST_EQUAL(answers[i].size(), multiStr.size());
                for (size_t j = 0; j < answers[i].size(); ++j)
                {
                    MultiChar multiChar = multiStr[j];
                    const string& oneStr = answers[i][j];
                    INDEXLIB_TEST_EQUAL(oneStr.size(), multiChar.size());
                    for (size_t k = 0; k < oneStr.size(); ++k)
                    {
                        INDEXLIB_TEST_EQUAL(oneStr[k], multiChar[k]);
                    }
                }
            }
            
            //test get data length
            size_t dataSize = VarNumAttributeFormatter::GetEncodedCountLength(
                    answers[i].size());
            if (answers[i].size() > 0)
            {
                dataSize = dataSize + sizeof(uint8_t) + 
                answers[i].size() * VarNumAttributeFormatter::GetOffsetItemLength(lastOffset) + 
                dataLength;
            }
            ASSERT_EQ(dataSize, segReader.GetDataLength((docid_t)i)) << i;
        }
    }

private:
    AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeSegmentReaderTest, TestCaseForOneDoc);
INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeSegmentReaderTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeSegmentReaderTest, TestCaseForOffsetOver64K);


IE_NAMESPACE_END(index);
