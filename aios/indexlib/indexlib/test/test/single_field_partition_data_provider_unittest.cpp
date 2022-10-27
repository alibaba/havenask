#include "indexlib/test/test/single_field_partition_data_provider_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, SingleFieldPartitionDataProviderTest);

SingleFieldPartitionDataProviderTest::SingleFieldPartitionDataProviderTest()
{
}

SingleFieldPartitionDataProviderTest::~SingleFieldPartitionDataProviderTest()
{
}

void SingleFieldPartitionDataProviderTest::CaseSetUp()
{
}

void SingleFieldPartitionDataProviderTest::CaseTearDown()
{
}

void SingleFieldPartitionDataProviderTest::TestCreatePkIndex()
{
    SingleFieldPartitionDataProvider provider;
    string fieldTypeStr = "string";
    provider.Init(GET_TEST_DATA_PATH(), fieldTypeStr, SFP_PK_INDEX);
    provider.Build("0,1,2,3#4,5,6,7", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    Version version = partitionData->GetVersion();
    ASSERT_EQ((size_t)2, version.GetSegmentCount());
    
    provider.Build("0,1,2,3#4,5,6,7", SFP_REALTIME);
    
    partitionData = provider.GetPartitionData();
    version = partitionData->GetVersion();
    ASSERT_EQ((size_t)3, version.GetSegmentCount());
}

void SingleFieldPartitionDataProviderTest::TestCreateIndex()
{
    SingleFieldPartitionDataProvider provider;
    string fieldTypeStr = "string";
    provider.Init(GET_TEST_DATA_PATH(), fieldTypeStr, SFP_INDEX);
    provider.Build("add:0,add:1,2,3#add:4,5,6,7", SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    Version version = partitionData->GetVersion();
    ASSERT_EQ((size_t)2, version.GetSegmentCount());
    
    provider.Build("0,1,2,3#4,5,6,7", SFP_REALTIME);
    
    partitionData = provider.GetPartitionData();
    version = partitionData->GetVersion();
    ASSERT_EQ((size_t)3, version.GetSegmentCount());
}

IE_NAMESPACE_END(test);

