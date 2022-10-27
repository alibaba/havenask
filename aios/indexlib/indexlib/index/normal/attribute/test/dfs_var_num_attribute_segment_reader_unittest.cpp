#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/dfs_var_num_attribute_segment_reader.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class DfsVarNumAttributeSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForWriteRead()
    {
        TestWriteAndRead<uint32_t, uint32_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        TestWriteAndRead<uint64_t, uint64_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        TestWriteAndRead<float, float>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        TestWriteAndRead<int8_t, int8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        TestWriteAndRead<uint8_t, uint8_t>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        TestWriteAndRead<MultiChar, string>(VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD);
        uint64_t smallOffsetThreshold = 16;
        TestWriteAndRead<uint32_t, uint32_t>(smallOffsetThreshold);
        TestWriteAndRead<uint64_t, uint64_t>(smallOffsetThreshold);
        TestWriteAndRead<float, float>(smallOffsetThreshold);
        TestWriteAndRead<int8_t, int8_t>(smallOffsetThreshold);
        TestWriteAndRead<uint8_t, uint8_t>(smallOffsetThreshold);
        TestWriteAndRead<MultiChar, string>(smallOffsetThreshold);
    }

private:
    template <typename T, typename DataType>
    void TestWriteAndRead(uint64_t offsetThreshold)
    {
        TestWriteAndRead<T, DataType>(offsetThreshold, true);
        TestWriteAndRead<T, DataType>(offsetThreshold, false);
    }

    template <typename T, typename DataType>
    void TestWriteAndRead(uint64_t offsetThreshold, bool uniqCode)
    {
        TearDown();
        SetUp();

        AttributeConfigPtr attrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
        attrConfig->SetUniqEncode(uniqCode);
        attrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThreshold);

        VarNumAttributeWriter<T> writer(attrConfig);
        AttributeConvertorPtr convertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(attrConfig->GetFieldConfig()));
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);

        static const uint32_t DOC_NUM = 1000;
        size_t dataLen = 0;

        vector<vector<DataType> > data;

        for (uint32_t i = 0; i < DOC_NUM; ++i)
        {
            uint32_t valueLen = i * 3 % 10;
            dataLen += valueLen * sizeof(T);

            stringstream ss;
            vector<DataType> dataOneDoc;
            for (uint32_t j = 0; j < valueLen; j++)
            {
                if (j != 0)
                {
                    ss << MULTI_VALUE_SEPARATOR;
                }

                DataType value;
                MakeData<DataType>((i + j) * 3 % 128, value, ss);
                dataOneDoc.push_back(value);
            }
            data.push_back(dataOneDoc);
            string encodeValue = convertor->Encode(ss.str());
            writer.AddField((docid_t)i, autil::ConstString(encodeValue));
        }

        AttributeWriterHelper::DumpWriter(writer, mDir);

        DFSVarNumAttributeSegmentReader<T> segReader(attrConfig);
        SegmentInfo segInfo;
        segInfo.docCount = DOC_NUM;

        DirectoryPtr attrDirectory = GET_PARTITION_DIRECTORY()->GetDirectory(
                attrConfig->GetAttrName(), true);
        segReader.Open(attrDirectory, segInfo);
        uint32_t bufLen = sizeof(uint32_t) +
                          VAR_NUM_ATTRIBUTE_MAX_COUNT * sizeof(T);
        uint8_t *buf = new uint8_t[bufLen];
        for (uint32_t i = 0; i < DOC_NUM; ++i)
        {
            MultiValueType<T> multiValue;
            docid_t docId = (docid_t)i;
            uint32_t dataLen;
            INDEXLIB_TEST_TRUE(segReader.ReadDataAndLen(docId, buf, bufLen, dataLen));
            uint32_t recordNum = i * 3 % 10;
            // uint32_t recordNum = dataLen / sizeof(T);
            uint8_t *dataCur = buf;
            INDEXLIB_TEST_EQUAL(data[i].size(), (size_t)recordNum);

            multiValue.init(dataCur);

            for (size_t j = 0; j < data[i].size(); j++)
            {
                CheckEqual(data[i][j], multiValue[j]);
            }
        }
        delete []buf;
    }

    template <typename T>
    static void MakeData(int value, T&ret, stringstream &ss);

    template <typename T, typename DataType>
    void CheckEqual(const DataType &expect, const T &actual)
    {
        INDEXLIB_TEST_EQUAL(expect, actual);
    }


private:
    std::string mDir;
};


/////////////////////////////////////////

template <typename T>
void DfsVarNumAttributeSegmentReaderTest::MakeData(int value, T&ret, stringstream &ss)
{
    ss << value;
    ret = (T)value;
}
template <>
void DfsVarNumAttributeSegmentReaderTest::MakeData(int value, string &ret, stringstream &ss)
{
    ret = string('a', value % 20 + 'a');
    ss << ret;
}

template <>
void DfsVarNumAttributeSegmentReaderTest::CheckEqual(const string &expect, const MultiChar &actual)
{
    INDEXLIB_TEST_EQUAL(expect, string(actual.data(), actual.size()));
}

INDEXLIB_UNIT_TEST_CASE(DfsVarNumAttributeSegmentReaderTest, TestCaseForWriteRead);


IE_NAMESPACE_END(index);
