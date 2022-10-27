#include "indexlib/index/normal/summary/test/building_summary_reader_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/fake_partition_data_creator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuildingSummaryReaderTest);

BuildingSummaryReaderTest::BuildingSummaryReaderTest()
{
}

BuildingSummaryReaderTest::~BuildingSummaryReaderTest()
{
}

void BuildingSummaryReaderTest::CaseSetUp()
{
}

void BuildingSummaryReaderTest::CaseTearDown()
{
}

void BuildingSummaryReaderTest::TestSimpleProcess()
{
    string field = "pk:uint64:pk;string1:string;text1:text;multi_uint:uint32:true;multi_str:string:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", "pk;string1;multi_uint;multi_str");

    IndexPartitionOptions options;
    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEST_DATA_PATH(), false);

    string fullDocs = "cmd=add,pk=1,string1=A,multi_uint=1 2,multi_str=123 234;"
                      "cmd=add,pk=2,string1=B,multi_uint=2 3,multi_str=234 345;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=3,string1=B,multi_uint=3 4,multi_str=345 456,ts=0;"
                         "cmd=add,pk=4,string1=A,multi_uint=4 5,multi_str=456 567,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=5,string1=B,multi_uint=5 6,multi_str=567 678,ts=0;"
                           "cmd=add,pk=6,string1=A,multi_uint=6 7,multi_str=678 789,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,pk=7,string1=B,multi_uint=7 8,multi_str=789 8910,ts=0;"
                           "cmd=add,pk=8,string1=A,multi_uint=8 9,multi_str=8910 91011,ts=0;"
                           "cmd=add,pk=9,string1=C,multi_uint=9 10,multi_str=91011 101112,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,pk=10,string1=A,multi_uint=10 11,multi_str=101112 111213,ts=0;"
                          "cmd=add,pk=11,string1=C,multi_uint=11 12,multi_str=111213 121314,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    PrimaryKeyIndexReaderPtr pkReader =
        pdc.GetIndexPartition()->GetReader()->GetPrimaryKeyReader();

    LocalDiskSummaryReader summaryReader(schema->GetSummarySchema(), 0);
    ASSERT_TRUE(summaryReader.Open(pdc.CreatePartitionData(false), pkReader));

    CheckSummaryReader(summaryReader, "1", "1,A,12,123234");
    CheckSummaryReader(summaryReader, "2", "2,B,23,234345");
    CheckSummaryReader(summaryReader, "3", "3,B,34,345456");
    CheckSummaryReader(summaryReader, "4", "4,A,45,456567");
    CheckSummaryReader(summaryReader, "5", "5,B,56,567678");
    CheckSummaryReader(summaryReader, "6", "6,A,67,678789");
    CheckSummaryReader(summaryReader, "7", "7,B,78,7898910");
    CheckSummaryReader(summaryReader, "8", "8,A,89,891091011");
    CheckSummaryReader(summaryReader, "9", "9,C,910,91011101112");
    CheckSummaryReader(summaryReader, "10", "10,A,1011,101112111213");
    CheckSummaryReader(summaryReader, "11", "11,C,1112,111213121314");
}

void BuildingSummaryReaderTest::CheckSummaryReader(
        LocalDiskSummaryReader& reader,
        const string& pkStr, const string& summaryValueStr)
{
    Pool pool;
    document::SearchSummaryDocument sumDoc(&pool, 4);
    reader.GetDocumentByPkStr(pkStr, &sumDoc);

    vector<string> resultStrVec;
    StringUtil::fromString(summaryValueStr, resultStrVec, ",");

    const vector<ConstString*> fields = sumDoc.GetFields();
    ASSERT_EQ(fields.size(), resultStrVec.size());

    for (size_t i = 0; i < resultStrVec.size(); i++)
    {
        string value = string(fields[i]->data(), fields[i]->size());
        ASSERT_EQ(resultStrVec[i], value);
    }
}

IE_NAMESPACE_END(index);
