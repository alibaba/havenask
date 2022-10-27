#include <sstream>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

#define FIELD_NAME_TAG "price"

class SingleValueAttributeUpdaterTest : public INDEXLIB_TESTBASE
{
public:
    static const size_t DEFAULT_CHUNK_SIZE = 256 * 1024;

    SingleValueAttributeUpdaterTest()
    {
        mPool = new Pool(DEFAULT_CHUNK_SIZE);
    }

    ~SingleValueAttributeUpdaterTest()
    {
        delete mPool;
    }

    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH() + "/";
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForInt8()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;15,9";
        InnerTest<int8_t>(operationStr);
    }

    void TestCaseForInt16()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900";
        InnerTest<int16_t>(operationStr);
    }

    void TestCaseForInt32()
    {
        string operationStr = "15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10";
        InnerTest<int32_t>(operationStr);
    }

private:
    template<typename T>
    void InnerTest(const string& operationStr)
    {
        TearDown();
        SetUp();
        
        segmentid_t segId = 5;
        AttributeConfigPtr attrConfig = CreateAttrConfig();
        SingleValueAttributeUpdater<T> updater(NULL, segId, attrConfig);

        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        assert(convertor);

        vector<pair<docid_t, string> > operationVect;
        map<docid_t, T> answerMap;
        CreateOperationAndAnswerVect(operationStr, operationVect, answerMap);
     
        for (size_t i = 0; i < operationVect.size(); ++i)
        {
            pair<docid_t, string>& op = operationVect[i];
            ConstString encodeValue = convertor->Encode(ConstString(op.second), mPool);
            common::AttrValueMeta meta = convertor->Decode(encodeValue);
            updater.Update(op.first, meta.data);
        }
        updater.Dump(GET_PARTITION_DIRECTORY());
        Check(segId, answerMap, attrConfig->GetCompressType().HasPatchCompress());
    }

    AttributeConfigPtr CreateAttrConfig()
    {
        FieldConfigPtr fieldConfig(new FieldConfig(FIELD_NAME_TAG, ft_integer, false));
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        return attrConfig;
    }

    template<typename T>
    void CreateOperationAndAnswerVect(const string& operationStr, 
            vector<pair<docid_t, string> >& operationVect,
            map<docid_t, T>& answerMap)
    {
        StringTokenizer st;
        st.tokenize(operationStr, ";");
        for (size_t i = 0; i < st.getNumTokens(); ++i) 
        {
            StringTokenizer subSt;
            subSt.tokenize(st[i], ",");

            docid_t docId = StringUtil::fromString<docid_t>(subSt[0]);
            T value = StringUtil::fromString<T>(subSt[1]);
            operationVect.push_back(make_pair(docId, subSt[1]));
            answerMap[docId] = value;
        }
    }

    template<typename T>
    void Check(segmentid_t segId, map<docid_t, T>& answerMap, bool isPatchCompressed)
    {
        SinglePatchFile patchFile(segId, isPatchCompressed);
        stringstream ss;
        ss << mDir << FIELD_NAME_TAG << "/" << segId << "." << ATTRIBUTE_PATCH_FILE_NAME;
        patchFile.Open(ss.str());
        INDEXLIB_TEST_EQUAL(segId, patchFile.GetSegmentId());
        for (size_t i = 0; i < answerMap.size(); ++i)
        {
            INDEXLIB_TEST_EQUAL(true, patchFile.HasNext());
            patchFile.Next();
            docid_t docId = patchFile.GetCurDocId();
            T expectedValue = answerMap[docId];
            T readValue = patchFile.GetPatchValue<T>();
            INDEXLIB_TEST_EQUAL(expectedValue, readValue);
        }
        INDEXLIB_TEST_EQUAL(false, patchFile.HasNext());
    }

private:
    string mDir;
    Pool* mPool;
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeUpdaterTest, TestCaseForInt8);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeUpdaterTest, TestCaseForInt16);
INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeUpdaterTest, TestCaseForInt32);

IE_NAMESPACE_END(index);
