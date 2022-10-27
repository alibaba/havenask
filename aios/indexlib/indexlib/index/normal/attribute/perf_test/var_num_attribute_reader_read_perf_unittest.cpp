#include "indexlib/index/normal/attribute/perf_test/var_num_attribute_reader_read_perf_unittest.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/util/timer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeReaderReadPerfTest);

VarNumAttributeReaderReadPerfTest::VarNumAttributeReaderReadPerfTest()
{
}

VarNumAttributeReaderReadPerfTest::~VarNumAttributeReaderReadPerfTest()
{
}

void VarNumAttributeReaderReadPerfTest::CaseSetUp()
{
}

void VarNumAttributeReaderReadPerfTest::CaseTearDown()
{
}

void VarNumAttributeReaderReadPerfTest::TestRead()
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
    int16_t updateField[2] = {1, 1};
    for (size_t i = 0; i < totalDocCount / 2; i++)
    {
        attrReader.UpdateField((docid_t)i, (uint8_t*)updateField, sizeof(updateField));
    }

    cout << "read normal data" << endl; //8.4ms 
    InnerRead(attrReader, totalDocCount/2, totalDocCount);
    cout << "read slice data" << endl; //10ms // 114237922ns
    InnerRead(attrReader, 0, totalDocCount/2);
}

void VarNumAttributeReaderReadPerfTest::InnerRead(
        const VarNumAttributeReader<int16_t>& attrReader, 
        docid_t startDocId, docid_t endDocId)
{
    MultiValueType<int16_t> value;
    uint64_t startReadTime = Timer::GetCurrentTimeInNanoSeconds();
    for (docid_t docid = startDocId; docid < endDocId; docid++)
    {
        attrReader.Read(docid, value);
    }

    for (docid_t docid = startDocId; docid < endDocId; docid+=4)
    {
        attrReader.Read(docid, value);
    }

    for (docid_t docid = startDocId; docid < endDocId; docid+=8)
    {
        attrReader.Read(docid, value);
    }
    uint64_t endReadTime = Timer::GetCurrentTimeInNanoSeconds();
    cout << " read used time:" << endReadTime - startReadTime << "ns" << endl;
}

IE_NAMESPACE_END(index);

