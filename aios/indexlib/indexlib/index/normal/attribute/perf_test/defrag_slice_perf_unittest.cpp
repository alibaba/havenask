#include "indexlib/index/normal/attribute/perf_test/defrag_slice_perf_unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/timer.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefragSlicePerfTest);

DefragSlicePerfTest::DefragSlicePerfTest()
{
}

DefragSlicePerfTest::~DefragSlicePerfTest()
{
}

void DefragSlicePerfTest::CaseSetUp()
{
}

void DefragSlicePerfTest::CaseTearDown()
{
}

void DefragSlicePerfTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "int16:true:true", SFP_ATTRIBUTE);
    stringstream docs;
    size_t totalDocCount = 1024 * 1024;
    for (size_t i = 0; i < totalDocCount - 1; i++)
    {
        docs << i << ",";
    }
    docs << totalDocCount - 1;
    provider.Build(docs.str(), SFP_OFFLINE);

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    PartitionDataPtr partitionData = provider.GetPartitionData();

    VarNumAttributeReader<int16_t> attrReader;
    attrReader.Open(attrConfig, partitionData);
    int16_t updateField1[32] = {31, 1};
    int16_t updateField2[32] = {31, 2};
    uint64_t startUpdateTime = Timer::GetCurrentTimeInNanoSeconds();
    for (size_t i = 0; i < totalDocCount; i++)
    {
        attrReader.UpdateField((docid_t)i, (uint8_t*)updateField1, sizeof(updateField1));
    }
    uint64_t endUpdateTime = Timer::GetCurrentTimeInNanoSeconds();
    cout << "update 1M doc used time:" << endUpdateTime - startUpdateTime << "ns" << endl;


    for (size_t i = 0; i < totalDocCount / 2 - 1; i++)
    {
        attrReader.UpdateField((docid_t)i, (uint8_t*)updateField2, sizeof(updateField2));
    }
    cout << "defrag begin" << endl;

    uint64_t startDefragTime = Timer::GetCurrentTimeInNanoSeconds();
    attrReader.UpdateField((docid_t)totalDocCount / 2, (uint8_t*)updateField2, 64);
    uint64_t endDefragTime = Timer::GetCurrentTimeInNanoSeconds();
    //defrag used 45ms
    cout << "defrag 1 slice used time:" << endDefragTime - startDefragTime << "ns" << endl;
}

IE_NAMESPACE_END(index);

