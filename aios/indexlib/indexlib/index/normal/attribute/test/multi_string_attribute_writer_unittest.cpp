#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/multi_string_attribute_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class MultiStringAttributeWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiStringAttributeWriterTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneDocument()
    {
        string docStr = string("abc") + MULTI_VALUE_SEPARATOR + 
                        "aaa" + MULTI_VALUE_SEPARATOR + 
                        "test" + MULTI_VALUE_SEPARATOR + 
                        "";
        uint32_t dataLen = 20;
        vector<string> fields;
        fields.push_back(docStr);

        TestBuild(1, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t));
        TestBuild(1, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);

        uint64_t smallOffsetThreshold = 16;
        TestBuild(1, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t));
        TestBuild(1, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t), true);
    }

    void TestCaseForNormalWrite()
    {
        static const uint32_t DOC_COUNT = 100;
        uint32_t dataLen;
        vector<string> fields;
        ASSERT_TRUE(MakeData(DOC_COUNT, dataLen, fields));

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
        ASSERT_TRUE(MakeEncodeData(DOC_COUNT, UNIQ_COUNT, dataLen, fields));

        TestBuild(DOC_COUNT, dataLen, fields, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, sizeof(uint32_t), true);

        uint64_t smallOffsetThreshold = 16;
        TestBuild(DOC_COUNT, dataLen, fields, smallOffsetThreshold, sizeof(uint64_t), true);
    }

private:
    bool MakeData(uint32_t docCount, uint32_t& dataLen, vector<string>& fields)
    {
        dataLen = 0;
        static uint32_t MAX_OFFSET = std::numeric_limits<uint16_t>::max();
	(void)MAX_OFFSET;
        for (uint32_t i = 0; i < docCount; ++i)
        {
            uint32_t valueCount = i * 3 % 10;
            dataLen += VarNumAttributeFormatter::GetEncodedCountLength(
                    valueCount); //value count
            uint32_t length = 0;
            uint32_t lastOffset = 0;
            stringstream ss;
            for (uint32_t j = 0; j < valueCount; ++j)
            {
                if (j != 0)
                {
                    ss << MULTI_VALUE_SEPARATOR;
                }
                uint32_t valueLen = (j * 3 % 10) + 1;
                uint32_t encodeLen = 
                    VarNumAttributeFormatter::GetEncodedCountLength(valueLen);
                if (length + encodeLen + valueLen >= MAX_OFFSET)
                {
                    return false;
                }

                lastOffset = length;
                length += (valueLen + encodeLen);

                for (uint32_t k = 0; k < valueLen; ++k)
                {
                    ss << (char)((i + j + k) % 26 + 'a');
                }
            }

            if (valueCount > 0)
            {
                dataLen += sizeof(uint8_t);
                dataLen += VarNumAttributeFormatter::GetOffsetItemLength(
                        lastOffset) * valueCount; //offset for each value
                dataLen += length;
            }
            fields.push_back(ss.str());
        }
        return true;
    }

    bool MakeEncodeData(uint32_t docCount, uint32_t uniqCount, uint32_t& dataLen, vector<string>& fields)
    {
        dataLen = 0;
        static uint32_t MAX_OFFSET = std::numeric_limits<uint16_t>::max();
	(void)MAX_OFFSET;
        vector<string> uniqfields;
        for (uint32_t i = 0; i < uniqCount; ++i)
        {
            uint32_t valueCount = i % uniqCount;
            dataLen += VarNumAttributeFormatter::GetEncodedCountLength(
                    valueCount); //value count

            uint32_t length = 0;
            uint32_t lastOffset = 0;
            stringstream ss;
            for (uint32_t j = 0; j < valueCount; ++j)
            {
                if (j != 0)
                {
                    ss << MULTI_VALUE_SEPARATOR;
                }
                uint32_t valueLen = (j * 3 % 10) + 1;

                uint32_t encodeLen = 
                    VarNumAttributeFormatter::GetEncodedCountLength(valueLen);
                if (length + encodeLen + valueLen >= MAX_OFFSET)
                {
                    return false;
                }

                lastOffset = length;
                length += (valueLen + encodeLen);
                for (uint32_t k = 0; k < valueLen; ++k)
                {
                    ss << (char)((i + j + k) % 26 + 'a');
                }
            }

            if (valueCount > 0)
            {
                dataLen += sizeof(uint8_t);
                dataLen += VarNumAttributeFormatter::GetOffsetItemLength(
                        lastOffset) * valueCount; //offset for each value
                dataLen += length;
            }
            uniqfields.push_back(ss.str());
        }

        for (uint32_t i = 0; i < docCount; ++i)
        {
            fields.push_back(uniqfields[i % uniqCount]);
        }
        return true;
    }

    void TestBuild(uint32_t docCount, uint32_t dataLen, const vector<string>& fields, 
            uint64_t offsetThreshold, int64_t offsetUnitSize, bool encode = false)
    {
        IndexTestUtil::ResetDir(mDir);
        AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<MultiChar>(true);
        if (encode)
        {
            attrConfig->SetUniqEncode(true);
        }
        attrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThreshold);

        AttributeConvertorPtr convertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        MultiStringAttributeWriter writer(attrConfig);
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);

        for (size_t i = 0; i < fields.size(); ++i)
        {
            string encodeStr = convertor->Encode(fields[i]);
            writer.AddField((docid_t)i, ConstString(encodeStr));
        }
        AttributeWriterHelper::DumpWriter(writer, mDir);

        string dataFilePath = mDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_DATA_FILE_NAME;
        string offsetFilePath = mDir + attrConfig->GetAttrName() + "/" + ATTRIBUTE_OFFSET_FILE_NAME;

        FileMeta fileMeta;
        fslib::ErrorCode err = FileSystem::getFileMeta(dataFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_EQUAL((int64_t)dataLen, fileMeta.fileLength);

        err = FileSystem::getFileMeta(offsetFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        INDEXLIB_TEST_EQUAL(offsetUnitSize * (docCount + 1), fileMeta.fileLength);
    }

private:
    string mDir;
};

INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeWriterTest, TestCaseForOneDocument);
INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeWriterTest, TestCaseForNormalWrite);
INDEXLIB_UNIT_TEST_CASE(MultiStringAttributeWriterTest, TestCaseForEncodeWrite);


IE_NAMESPACE_END(index);
