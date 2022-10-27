#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_writer.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "autil/mem_pool/Pool.h"

#include "fslib/fs/FileSystem.h"
#include "fslib/fs/File.h"
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace std::tr1;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);

class StringAttributeReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(StringAttributeReaderTest);

    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForWriteAndRead()
    {
        TestRead(false);
        TestRead(true);
    }

    void TestCaseForGetType()
    {
        StringAttributeReader reader;
        INDEXLIB_TEST_EQUAL(AT_STRING,reader.GetType());
    }

    void TestCaseForReadBuildingSegmentReader()
    {
        SingleFieldPartitionDataProvider provider;
        provider.Init(GET_TEST_DATA_PATH(), "string", SFP_ATTRIBUTE);
        provider.Build("", SFP_OFFLINE);

        string buildingValue = "helloworld";
        provider.Build(buildingValue, SFP_REALTIME);

        AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
        StringAttributeReader reader;
        reader.Open(attrConfig, provider.GetPartitionData());

        MultiValueType<char> value;
        reader.Read(0, value);
        ASSERT_EQ((uint32_t)10, value.size());
        
        string resultStr;
        reader.Read(0, resultStr);
        ASSERT_EQ(buildingValue, resultStr);

        typedef AttributeIteratorTyped<
            MultiValueType<char>, 
            AttributeReaderTraits<MultiValueType<char> > > AttributeIterator;
        AttributeIterator* iterator = 
        dynamic_cast<AttributeIterator*>(
                reader.CreateIterator(&_pool));
        ASSERT_TRUE(iterator);
        iterator->Seek(0, value);
        ASSERT_EQ((uint32_t)10, value.size());
        resultStr = std::string(value.data(), value.size());
        ASSERT_EQ(buildingValue, resultStr);
        IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, iterator);
    }

private:
    void TestRead(bool uniqEncode)
    {
        TearDown();
        SetUp();
        vector<uint32_t> docCounts;
        docCounts.push_back(100);
        docCounts.push_back(33);
        docCounts.push_back(22);
        vector<string> ans;
        FullBuild(docCounts, ans, uniqEncode);

        index_base::PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), docCounts.size());
        StringAttributeReader reader;
        reader.Open(mAttrConfig, partitionData);
        CheckRead(reader, ans);
        CheckIterator(reader, ans);
    }
    
    void FullBuild(const vector<uint32_t>& docCounts, vector<string>& ans, bool uniqEncode)
    {
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<MultiChar>(false);
        mAttrConfig->SetUniqEncode(uniqEncode);
        for (uint32_t i = 0; i < docCounts.size(); i++)
        {
            segmentid_t segId = i;
            CreateDataForOneSegment(segId, docCounts[i], ans);
        }
    }

    void CreateDataForOneSegment(segmentid_t segId, uint32_t docCount, 
                                 vector<string>& ans)
    {
        StringAttributeWriter writer(mAttrConfig);
        AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(mAttrConfig->GetFieldConfig()));
        writer.SetAttrConvertor(convertor);
        writer.Init(FSWriterParamDeciderPtr(), NULL);
        autil::mem_pool::Pool pool;
        for (uint32_t i = 0; i < docCount; ++i)
        {
            uint32_t valueLen = (i + rand()) * 3 % 10;
            string dataOneDoc;
            for (uint32_t j = 0; j < valueLen; j++)
            {
                dataOneDoc.append(1, (char)((i + j) * 3));
            }
            ans.push_back(dataOneDoc);
            ConstString encodeValue = convertor->Encode(ConstString(dataOneDoc), &pool);
            writer.AddField((docid_t)i, encodeValue);
        }

        file_system::DirectoryPtr directory = MAKE_SEGMENT_DIRECTORY(segId);
        file_system::DirectoryPtr attrDirectory = directory->MakeDirectory(
                ATTRIBUTE_DIR_NAME);
        util::SimplePool dumpPool;
        writer.Dump(attrDirectory, &dumpPool);

        SegmentInfo segInfo;
        segInfo.docCount = docCount;
        segInfo.Store(directory);
    }
    
    void CheckRead(const StringAttributeReader& reader, const vector<string>& ans)
    {
        for (docid_t i = 0; i < (docid_t)ans.size(); i++)
        {
            string str;
            INDEXLIB_TEST_TRUE(reader.Read(i, str));
            INDEXLIB_TEST_EQUAL(ans[i], str);

            MultiValueType<char> multiValue;
            INDEXLIB_TEST_TRUE(reader.Read(i, multiValue));
            INDEXLIB_TEST_EQUAL(ans[i].size(), multiValue.size());

            for (size_t j = 0; j < ans[i].size(); j++)
            {
                INDEXLIB_TEST_EQUAL(ans[i][j], multiValue[j]);
            }
        }
    }

    void CheckIterator(const StringAttributeReader& reader,
                       const vector<string>& ans)
    {
        AttributeIteratorBase* attrIter = reader.CreateIterator(&_pool);
        StringAttributeReader::AttributeIterator* typedIter = 
            dynamic_cast<StringAttributeReader::AttributeIterator*> (attrIter);

        for (docid_t i = 0; i < (docid_t)ans.size(); i++)
        {
            MultiValueType<char> multiValue;
            INDEXLIB_TEST_TRUE(typedIter->Seek(i, multiValue));
            INDEXLIB_TEST_EQUAL(ans[i].size(), multiValue.size());

            for (size_t j = 0; j < ans[i].size(); j++)
            {
                INDEXLIB_TEST_EQUAL(ans[i][j], multiValue[j]);
            }
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, attrIter);
    }
private:
    config::AttributeConfigPtr mAttrConfig;
    autil::mem_pool::Pool _pool;
};

INDEXLIB_UNIT_TEST_CASE(StringAttributeReaderTest, TestCaseForWriteAndRead);
INDEXLIB_UNIT_TEST_CASE(StringAttributeReaderTest, TestCaseForGetType);
INDEXLIB_UNIT_TEST_CASE(StringAttributeReaderTest, TestCaseForReadBuildingSegmentReader);

IE_NAMESPACE_END(index);
