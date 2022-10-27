#include "indexlib/index/normal/attribute/test/lazy_load_single_value_attribute_reader_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/config/attribute_config.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);

LazyLoadSingleValueAttributeReaderTest::LazyLoadSingleValueAttributeReaderTest()
{
}

LazyLoadSingleValueAttributeReaderTest::~LazyLoadSingleValueAttributeReaderTest()
{
}

void LazyLoadSingleValueAttributeReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void LazyLoadSingleValueAttributeReaderTest::CaseTearDown()
{
}

void LazyLoadSingleValueAttributeReaderTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    string docValues = "3,5,6#7#8#delete:4#9,10,11";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadSingleValueAttributeReader<int32_t>());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader, "3,5,6,7,8,9,10,11");
}

void LazyLoadSingleValueAttributeReaderTest::TestReadWithPatchFiles()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    string docValues = "3,5,6#update_field:0:2,7#update_field:0:1,8#delete:4#9,10,11";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadSingleValueAttributeReader<int32_t>());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader, "1,5,6,7,8,9,10,11");
}

void LazyLoadSingleValueAttributeReaderTest::CheckValues(const DirectoryPtr& rootDir,
                                                         const AttributeReaderPtr& attrReader,
                                                         const string& expectValueString)
{
    assert(attrReader);
    assert(rootDir);
    const IndexlibFileSystemPtr& fileSystem = rootDir->GetFileSystem();
    
    vector<string> expectValues;
    StringUtil::fromString(expectValueString, expectValues, ",");
    
    docid_t docId = 0;
    for (size_t i = 0; i < expectValues.size(); ++i)
    {
        string attrValue;
        ASSERT_TRUE(attrReader->Read(docId++, attrValue));
        ASSERT_EQ(expectValues[i], attrValue);
        // check only one segment's attribute data file is opened
        const StorageMetrics& storageMetrics = fileSystem->GetStorageMetrics(FSST_LOCAL);
        ASSERT_EQ(int64_t(1), storageMetrics.GetFileCount(FSFT_MMAP));
    }
}

IE_NAMESPACE_END(attribute);

