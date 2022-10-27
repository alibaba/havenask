#include "indexlib/index/normal/attribute/test/lazy_load_var_num_attribute_reader_unittest.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_string_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_location_attribute_reader.h"
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

LazyLoadVarNumAttributeReaderTest::LazyLoadVarNumAttributeReaderTest()
{
}

LazyLoadVarNumAttributeReaderTest::~LazyLoadVarNumAttributeReaderTest()
{
}

void LazyLoadVarNumAttributeReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void LazyLoadVarNumAttributeReaderTest::CaseTearDown()
{
}

void LazyLoadVarNumAttributeReaderTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32:true", SFP_ATTRIBUTE);
    string docValues = "34,56,67#78#89#delete:4#910,1011,1112";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadVarNumAttributeReader<int32_t>());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "34,56,67,78,89,910,1011,1112", false); 
}

void LazyLoadVarNumAttributeReaderTest::TestReadWithInMemSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32:true", SFP_ATTRIBUTE);
    provider.Build("", SFP_OFFLINE);
    string docValues = "34,56,67#78#89#delete:4#910,1011,1112";
    provider.Build(docValues, SFP_REALTIME);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadVarNumAttributeReader<int32_t>());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "34,56,67,78,89,910,1011,1112", false, false); 
}

void LazyLoadVarNumAttributeReaderTest::TestReadWithPatchFiles()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32:true:true", SFP_ATTRIBUTE);
    string docValues = "34,56,67#update_field:0:23,78#89,update_field:0:12#delete:4#910,1011,1112";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadVarNumAttributeReader<int32_t>());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "12,56,67,78,89,910,1011,1112", true); 
}

void LazyLoadVarNumAttributeReaderTest::TestReadStringAttribute()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string:false:true", SFP_ATTRIBUTE);
    string docValues = "hi5,hi1#update_field:0:hi2#hi3,update_field:0:hi0#delete:2#hi4,hi5,hi6";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadStringAttributeReader());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "hi0,hi1,hi3,hi4,hi5,hi6", true);
}

void LazyLoadVarNumAttributeReaderTest::TestReadMultiStringAttribute()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "string:true:true", SFP_ATTRIBUTE);
    string docValues = "hi5hi6,hi1hi2#update_field:0:hi2hi3#hi3hi4,update_field:0:hi0hi1"
        "#delete:2#hi5,hi6hi7,hi8hi9";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadMultiStringAttributeReader());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "hi0hi1,hi1hi2,hi3hi4,hi5,hi6hi7,hi8hi9", true);    
}

void LazyLoadVarNumAttributeReaderTest::TestReadLocationAttribute()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "location:false:false", SFP_ATTRIBUTE);
    string docValues = "10.3 10.1,10.1 10.2#10.2 10.3#10.3 10.4#delete:2#10.5 10.6,10.6 10.7";
    provider.Build(docValues, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    AttributeReaderPtr attrReader(new LazyLoadLocationAttributeReader());
    ASSERT_TRUE(attrReader->Open(attrConfig, partitionData));
    CheckValues(partitionData->GetRootDirectory(), attrReader,
                "10.3 10.1,10.1 10.2,10.2 10.3,10.3 10.4,10.5 10.6,10.6 10.7", false);    
}    

void LazyLoadVarNumAttributeReaderTest::CheckValues(const DirectoryPtr& rootDir,
                                                    const AttributeReaderPtr& attrReader,
                                                    const string& expectValueString,
                                                    bool isAttrUpdatable,
                                                    bool checkLoadMode)
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
        const StorageMetrics& storageMetrics = fileSystem->GetStorageMetrics(FSST_LOCAL);
        // check only one segment's attribute data file is opened
        // attribute offset & data files are loaded by MMAP
        if (checkLoadMode)
        {
            ASSERT_EQ(int64_t(2), storageMetrics.GetFileCount(FSFT_MMAP)); 
        }
    }
}

IE_NAMESPACE_END(index);

