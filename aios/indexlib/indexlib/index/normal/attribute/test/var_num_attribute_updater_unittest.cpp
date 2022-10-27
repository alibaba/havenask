
#include <sstream>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);

#define FIELD_NAME_TAG "price"

class VarNumAttributeUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 1024;

    VarNumAttributeUpdaterTest()
    {
    }

    ~VarNumAttributeUpdaterTest()
    {
    }

    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    //case input format: "docId_1,value_1|value_2|value_3;docId_2,value2_1"
    void TestSimple()
    {
        string operationStr = "3,10";
        InnerTest<int8_t>(operationStr, ft_int8);
    }

    void TestMultiValue()
    {
        string operationStr = "3,10|11|120;4,13|17";
        InnerTest<int8_t>(operationStr, ft_int8);
    }

    void TestCaseForInt8()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;15,9";
        InnerTest<int8_t>(operationStr, ft_int8);
    }

    void TestCaseForInt16()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900";
        InnerTest<int16_t>(operationStr, ft_int16);
    }

    void TestCaseForInt32()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10";
        InnerTest<int32_t>(operationStr, ft_int32);
    }

    void TestCaseForString()
    {
        TearDown();
        SetUp();

        segmentid_t segId = 6;
        FieldConfigPtr fieldConfig(new FieldConfig(FIELD_NAME_TAG, ft_string, false));
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        attrConfig->SetUpdatableMultiValue(true);
        StringAttributeUpdater updater(NULL, segId, attrConfig);
        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        assert(convertor);
        updater.Update(1, GetDecodedValue(convertor, "abc"));
        updater.Update(1, GetDecodedValue(convertor, "1234"));
        updater.Update(10, GetDecodedValue(convertor, "def"));
        updater.Dump(GET_PARTITION_DIRECTORY());

        size_t bufferLen = sizeof(uint16_t)
                           + sizeof(char) * VAR_NUM_ATTRIBUTE_MAX_COUNT;
        uint8_t buffer[bufferLen];

        VarNumAttributePatchFile patchFile(segId, attrConfig);

        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr patchDirectory = directory->GetDirectory(FIELD_NAME_TAG, true);
        stringstream ss;
        ss << segId << "." << ATTRIBUTE_PATCH_FILE_NAME;

        patchFile.Open(patchDirectory, ss.str());
        EXPECT_EQ(segId, patchFile.GetSegmentId());

        EXPECT_TRUE(patchFile.HasNext());
        patchFile.Next();
        EXPECT_EQ(1, patchFile.GetCurDocId());
        patchFile.GetPatchValue<char>(buffer, bufferLen);

        size_t encodeCountLen = 0;
        uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                (const char*)buffer, encodeCountLen);
        char* actualValue = (char*)(buffer + encodeCountLen);
        EXPECT_EQ((uint32_t)4, recordNum);
        EXPECT_EQ('1', actualValue[0]);
        EXPECT_EQ('2', actualValue[1]);
        EXPECT_EQ('3', actualValue[2]);
        EXPECT_EQ('4', actualValue[3]);

        EXPECT_TRUE(patchFile.HasNext());
        patchFile.Next();
        EXPECT_EQ(10, patchFile.GetCurDocId());
        patchFile.GetPatchValue<char>(buffer, bufferLen);

        recordNum = VarNumAttributeFormatter::DecodeCount(
                (const char*)buffer, encodeCountLen);
        actualValue = (char*)(buffer + encodeCountLen);
        EXPECT_EQ((uint32_t)3, recordNum);
        EXPECT_EQ('d', actualValue[0]);
        EXPECT_EQ('e', actualValue[1]);
        EXPECT_EQ('f', actualValue[2]);
        
        EXPECT_FALSE(patchFile.HasNext());
    }

    void TestCaseForMultiString()
    {
        TearDown();
        SetUp();

        segmentid_t segId = 6;
        AttributeConfigPtr attrConfig = CreateAttrConfig(ft_string);
        VarNumAttributeUpdater<MultiChar> updater(NULL, segId, attrConfig);
        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        assert(convertor);

        updater.Update(1, GetDecodedValue(convertor, "abc"));
        updater.Update(1, GetDecodedValue(convertor, "1234"));
        updater.Update(10, GetDecodedValue(convertor, "defhijk"));
        updater.Dump(GET_PARTITION_DIRECTORY());

        size_t bufferLen = sizeof(uint32_t)
                           + sizeof(char) * VAR_NUM_ATTRIBUTE_MAX_COUNT;
        uint8_t buffer[bufferLen];

        VarNumAttributePatchFile patchFile(segId, attrConfig);

        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr patchDirectory = 
            directory->GetDirectory(FIELD_NAME_TAG, true);
        stringstream ss;
        ss << segId << "." << ATTRIBUTE_PATCH_FILE_NAME;
        patchFile.Open(patchDirectory, ss.str());
        EXPECT_EQ(segId, patchFile.GetSegmentId());

        EXPECT_TRUE(patchFile.HasNext());
        patchFile.Next();
        EXPECT_EQ(1, patchFile.GetCurDocId());
        patchFile.GetPatchValue<MultiChar>(buffer, bufferLen);

        size_t encodeCountLen = 0;
        uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                (const char*)buffer, encodeCountLen);
        uint8_t* actualValue = buffer + encodeCountLen;
        EXPECT_EQ((uint32_t)1, recordNum);
        // offset length
        EXPECT_EQ((uint8_t)1, *(uint8_t*)(&actualValue[0]));
        // offset[0]
        EXPECT_EQ((uint8_t)0, *(uint8_t*)(&actualValue[1]));
        // item header 
        EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[2]));
        EXPECT_EQ('1', actualValue[3]);
        EXPECT_EQ('2', actualValue[4]);
        EXPECT_EQ('3', actualValue[5]);
        EXPECT_EQ('4', actualValue[6]);


        EXPECT_TRUE(patchFile.HasNext());
        patchFile.Next();
        EXPECT_EQ(10, patchFile.GetCurDocId());
        patchFile.GetPatchValue<MultiChar>(buffer, bufferLen);
        recordNum = VarNumAttributeFormatter::DecodeCount(
                (const char*)buffer, encodeCountLen);
        EXPECT_EQ((uint16_t)2, recordNum);
        actualValue = buffer + encodeCountLen;
        // offset length
        EXPECT_EQ((uint8_t)1, *(uint8_t*)(&actualValue[0]));
        // offset[0]
        EXPECT_EQ((uint8_t)0, *(uint8_t*)(&actualValue[1]));
        // offset[1]
        EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[2]));

        // item1
        EXPECT_EQ((uint8_t)3, *(uint8_t*)(&actualValue[3]));
        EXPECT_EQ('d', actualValue[4]);
        EXPECT_EQ('e', actualValue[5]);
        EXPECT_EQ('f', actualValue[6]);

        // item2
        EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[7]));
        EXPECT_EQ('h', actualValue[8]);
        EXPECT_EQ('i', actualValue[9]);
        EXPECT_EQ('j', actualValue[10]);
        EXPECT_EQ('k', actualValue[11]);

        EXPECT_FALSE(patchFile.HasNext());
    }

private:
    template<typename T>
    void InnerTest(const string& operationStr, FieldType fieldType)
    {
        TearDown();
        SetUp();

        segmentid_t segId = 6;
        AttributeConfigPtr attrConfig = CreateAttrConfig(fieldType);
        VarNumAttributeUpdater<T> updater(NULL, segId, attrConfig);
        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        assert(convertor);

        uint32_t maxValueLen = 0;
        vector<pair<docid_t, string> > operationVect;
        map<docid_t, vector<T> > answerMap;
        CreateOperationAndAnswerVect(operationStr, operationVect, answerMap);
        for (size_t i = 0; i < operationVect.size(); ++i)
        {
            pair<docid_t, string>& op = operationVect[i];
            ConstString encodeValue = GetDecodedValue(convertor, op.second); 
            maxValueLen = max(maxValueLen, (uint32_t)encodeValue.size());
            updater.Update(op.first, encodeValue);
        }
        updater.Dump(GET_PARTITION_DIRECTORY());

        Check(segId, answerMap, maxValueLen, attrConfig);
    }

    
    AttributeConfigPtr CreateAttrConfig(FieldType fieldType)
    {
        
        FieldConfigPtr fieldConfig(new FieldConfig(FIELD_NAME_TAG, fieldType, true));
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        attrConfig->SetUpdatableMultiValue(true);
        return attrConfig;
    }

    template<typename T>
    void CreateOperationAndAnswerVect(const string& operationStr, 
            vector<pair<docid_t, string> >& operationVect,
            map<docid_t, vector<T> >& answerMap)
    {
        StringTokenizer st;
        st.tokenize(operationStr, ";");
        for (size_t i = 0; i < st.getNumTokens(); ++i) 
        {
            StringTokenizer subSt;
            subSt.tokenize(st[i], ",");
            INDEXLIB_TEST_TRUE(2 == subSt.getNumTokens());
            docid_t docId = StringUtil::fromString<docid_t>(subSt[0]);
            vector<T> vctValue;
            string strValue;
            StringTokenizer multiSt;
            multiSt.tokenize(subSt[1], "|");
            for (size_t j = 0; j < multiSt.getNumTokens(); ++j)
            {
                vctValue.push_back(StringUtil::fromString<T>(multiSt[j]));
                strValue += multiSt[j];
                strValue += MULTI_VALUE_SEPARATOR;
            }
            operationVect.push_back(make_pair(docId, strValue));
            answerMap[docId] = vctValue;
        }
    }

    template<typename T>
    void Check(segmentid_t segId, map<docid_t, vector<T> >& answerMap,
               uint32_t maxValueLen, const AttributeConfigPtr& attrConfig)
    {
        VarNumAttributePatchFile patchFile(segId, attrConfig);
        file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
        file_system::DirectoryPtr patchDirectory = directory->GetDirectory(FIELD_NAME_TAG, true);
        stringstream ss;
        ss << segId << "." << ATTRIBUTE_PATCH_FILE_NAME;
        patchFile.Open(patchDirectory, ss.str());

        INDEXLIB_TEST_EQUAL(segId, patchFile.GetSegmentId());
        for (size_t i = 0; i < answerMap.size(); ++i)
        {
            INDEXLIB_TEST_EQUAL(true, patchFile.HasNext());
            patchFile.Next();
            docid_t docId = patchFile.GetCurDocId();
            vector<T> expectedValue = answerMap[docId];
            size_t bufferLen = sizeof(uint16_t)
                               + sizeof(T) * VAR_NUM_ATTRIBUTE_MAX_COUNT;
            uint8_t buffer[bufferLen];
            patchFile.GetPatchValue<T>(buffer, bufferLen);

            size_t encodeCountLen = 0;
            uint32_t recordNum = VarNumAttributeFormatter::DecodeCount(
                    (const char*)buffer, encodeCountLen);
            T* actualValue = (T*)(buffer + encodeCountLen);
            INDEXLIB_TEST_EQUAL(expectedValue.size(), recordNum);
            for (size_t j  = 0; j < expectedValue.size(); ++j) 
            {
                INDEXLIB_TEST_EQUAL(expectedValue[j], actualValue[j]);
            }
        }
        INDEXLIB_TEST_EQUAL(false, patchFile.HasNext());
        INDEXLIB_TEST_EQUAL(maxValueLen, patchFile.GetMaxPatchItemLen());
    }

private:
    ConstString GetDecodedValue(
            AttributeConvertorPtr convertor, const string& value)
    {
        ConstString encodeValue = convertor->Encode(ConstString(value), &mPool);
        common::AttrValueMeta meta = convertor->Decode(encodeValue);
        return meta.data;
    }

private:
    string mDir;
    Pool mPool;
};


INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestSimple);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestMultiValue);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestCaseForInt8);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestCaseForInt16);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestCaseForInt32);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestCaseForString);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeUpdaterTest, TestCaseForMultiString);

IE_NAMESPACE_END(index);
