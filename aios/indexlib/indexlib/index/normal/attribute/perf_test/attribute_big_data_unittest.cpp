#include "indexlib/index/normal/attribute/perf_test/attribute_big_data_unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/schema_maker.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeBigDataTest);

AttributeBigDataTest::AttributeBigDataTest()
{
}

AttributeBigDataTest::~AttributeBigDataTest()
{
}

void AttributeBigDataTest::CaseSetUp()
{
}

void AttributeBigDataTest::CaseTearDown()
{
}

void AttributeBigDataTest::TestSingleValueAttrWithBigDataInOneSegmentLongCaseTest()
{
    DoTestSingleValueAttrWithBigDataInOneSegment<double>("double", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<float>("float", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<int64_t>("int64", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint64_t>("uint64", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<int32_t>("int32", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint32_t>("uint32", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<int16_t>("int16", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint16_t>("uint16", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<int8_t>("int8", false);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint8_t>("uint8", false);
}

void AttributeBigDataTest::TestSingleValueAttrWithBigDataInOneSegmentEqualCompressLongCaseTest()
{
    DoTestSingleValueAttrWithBigDataInOneSegment<double>("double", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<float>("float", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<int64_t>("int64", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint64_t>("uint64", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<int32_t>("int32", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint32_t>("uint32", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<int16_t>("int16", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint16_t>("uint16", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<int8_t>("int8", true);
    DoTestSingleValueAttrWithBigDataInOneSegment<uint8_t>("uint8", true);
}

template<typename T>
void AttributeBigDataTest::DoTestSingleValueAttrWithBigDataInOneSegment(
        const string& fieldType, bool isEqualCompress)
{
    TearDown();
    SetUp();
    MakeSchema(fieldType, isEqualCompress);
    size_t maxDocCount = (size_t)std::numeric_limits<docid_t>::max();
    DirectoryPtr attrDir = MakePartition(maxDocCount);

    Pool pool;
    SingleValueAttributeWriter<T> writer(mAttrConfig);
    writer.Init(FSWriterParamDeciderPtr(), NULL);
    for (size_t i = 0; i < maxDocCount; ++i)
    {
        if ((i % (1024*1024*64) ) == 0) { cout << i << endl; }
        writer.AddField((docid_t)i, (T)(i % 131));
    }
    AttributeSegmentReaderPtr orgSegReader = writer.CreateInMemReader();
    tr1::shared_ptr<InMemSingleValueAttributeReader<T> > segReader =
        DYNAMIC_POINTER_CAST(InMemSingleValueAttributeReader<T>, orgSegReader);
    ASSERT_TRUE(segReader);
    for (size_t i = 0; i < maxDocCount; i += 13331)
    {
        T value;
        segReader->Read(i, value);
        ASSERT_EQ((T)(i % 131), value);
    }
    writer.Dump(attrDir, &pool);

    OnDiskPartitionDataPtr partData = PartitionDataCreator::CreateOnDiskPartitionData(
            GET_FILE_SYSTEM(), INVALID_VERSION, mPartDir->GetPath());
    SingleValueAttributeReader<T> reader;
    reader.Open(mAttrConfig, partData);
    for (size_t i = 0; i < maxDocCount; i += 13331)
    {
        T value;
        reader.Read(i, value);
        ASSERT_EQ((T)(i % 131), value);
    }
}

void AttributeBigDataTest::MakeSchema(const std::string& fieldType, bool isEqualCompress)
{
    string field = "attr:" + fieldType;
    if (isEqualCompress)
    {
        field +=  + ":false:false:equal";
    }
    string index = "pk:primarykey64:attr;";
    string attr = "attr;";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mAttrConfig = mSchema->GetAttributeSchema()->GetAttributeConfig("attr");
}

DirectoryPtr AttributeBigDataTest::MakePartition(uint32_t docCount)
{
    mPartDir = GET_PARTITION_DIRECTORY()->MakeInMemDirectory("part");
    PartitionStateMachine psm;
    string partPath = mPartDir->GetPath();
    psm.Init(mSchema, mOptions, partPath);
    psm.Transfer(BUILD_FULL, "", "", "");
    DirectoryPtr segDir = mPartDir->MakeDirectory("segment_0_level_0");

    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.Store(segDir);

    Version version(10);
    version.AddSegment(0);
    version.Store(mPartDir);

    return segDir->MakeDirectory("attribute");
}

IE_NAMESPACE_END(index);

