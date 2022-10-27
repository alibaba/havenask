#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class StringAttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(StringAttributeWriterTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneDocument()
    {
        string docStr = string("abcd");
        uint32_t dataLen = 5;

        vector<string> fields;
        fields.push_back(docStr);

        TestBuild(1, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
        TestBuild(1, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);

        uint64_t smallOffsetThreshold = 1;
        TestBuild(1, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t));
        TestBuild(1, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t), true);
    }

    void TestCaseForNormalWrite()
    {
        static const uint32_t DOC_COUNT = 100;
        uint32_t dataLen;
        vector<string> fields;
        MakeData(DOC_COUNT, dataLen, fields);

        TestBuild(DOC_COUNT, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));

        uint64_t smallOffsetThreshold = 16;
        TestBuild(DOC_COUNT, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t));
    }

    void TestCaseForEncodeWrite()
    {
        static const uint32_t DOC_COUNT = 100;
        static const uint32_t UNIQ_COUNT = (uint32_t)(DOC_COUNT * 0.25f);

        uint32_t dataLen;
        vector<string> fields;
        MakeEncodeData(DOC_COUNT, UNIQ_COUNT, dataLen, fields);

        TestBuild(DOC_COUNT, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);

        uint64_t smallOffsetThreshold = 16;
        TestBuild(DOC_COUNT, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t), true);
    }

private:
    void MakeData(uint32_t docCount, uint32_t& dataLen, vector<string>& fields)
    {
        dataLen = 0;
        for (uint32_t i = 0; i < docCount; ++i)
        {
            stringstream ss;
            uint32_t valueLen = (i * 3 % 10) + 1;
            for (uint32_t k = 0; k < valueLen; ++k)
            {
                ss << (char)((i + k) % 26 + 'a');
            }
            dataLen += VarNumAttributeFormatter::GetEncodedCountLength(valueLen);
            dataLen += valueLen;
            fields.push_back(ss.str());
        }
    }

    void MakeEncodeData(uint32_t docCount, uint32_t uniqCount, uint32_t& dataLen, vector<string>& fields)
    {
        dataLen = 0;
        vector<string> uniqfields;
        for (uint32_t i = 0; i < uniqCount; ++i)
        {
            stringstream ss;
            uint32_t valueLen = (i * 3 % 10) + 1;
            for (uint32_t k = 0; k < valueLen; ++k)
            {
                ss << (char)((i + k) % 26 + 'a');
            }
            dataLen += VarNumAttributeFormatter::GetEncodedCountLength(valueLen);
            dataLen += valueLen;
            uniqfields.push_back(ss.str());
        }

        for (uint32_t i = 0; i < docCount; ++i)
        {
            fields.push_back(uniqfields[i % uniqCount]);
        }
    }

    void TestBuild(uint32_t docCount, uint32_t dataLen, const vector<string>& fields, 
            uint64_t offsetThreshold, int64_t offsetUnitSize, bool encode = false)
    {
        IndexTestUtil::ResetDir(mRootDir);
        AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<MultiChar>(false);
        attrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThreshold);

        if (encode)
        {
            attrConfig->SetUniqEncode(true);
        }
        AttributeConvertorPtr convertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        StringAttributeWriter writer(attrConfig);
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);
        autil::mem_pool::Pool pool;
        for (size_t i = 0; i < fields.size(); ++i)
        {
            ConstString encodeStr = convertor->Encode(ConstString(fields[i]), &pool);
            writer.AddField((docid_t)i, encodeStr);
        }

        AttributeWriterHelper::DumpWriter(writer, mRootDir);

        string dataFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
        string offsetFilePath = mRootDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;

        FileMeta fileMeta;
        fslib::ErrorCode err = FileSystem::getFileMeta(dataFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_EQUAL((int64_t)dataLen, fileMeta.fileLength);

        err = FileSystem::getFileMeta(offsetFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_EQUAL(offsetUnitSize * (docCount + 1), fileMeta.fileLength);
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(StringAttributeWriterTest, TestCaseForOneDocument);
INDEXLIB_UNIT_TEST_CASE(StringAttributeWriterTest, TestCaseForNormalWrite);
INDEXLIB_UNIT_TEST_CASE(StringAttributeWriterTest, TestCaseForEncodeWrite);


IE_NAMESPACE_END(index);
