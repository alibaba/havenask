#include "indexlib/partition/test/aux_table_reader_unittest.h"
#include "indexlib/partition/aux_table_reader_creator.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, AuxTableReaderTest);

AuxTableReaderTest::AuxTableReaderTest()
{
}

AuxTableReaderTest::~AuxTableReaderTest()
{
}

void AuxTableReaderTest::CaseSetUp()
{
}

void AuxTableReaderTest::CaseTearDown()
{
}


void CheckValue(AuxTableReaderTyped<MultiValueType<float>>* auxReader,
                const std::string& key, const std::string& expectValueStr)
{
    MultiValueType<float> value;
    ASSERT_TRUE(auxReader->GetValue(key, value));

    vector<float> expectValues;
    StringUtil::fromString(expectValueStr, expectValues, " ");

    ASSERT_EQ(value.size(), expectValues.size());
    for (size_t i = 0; i < value.size(); i++)
    {
        ASSERT_EQ(expectValues[i], value[i]);
    }
}

void AuxTableReaderTest::TestReadFixLengthFloatFromIndexTable()
{
    string field = "key:string;payload:float:true:true:int8#127:4;price:uint32";
    string index = "pk:primarykey64:key";
    string attribute = "payload;price";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 2;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));

    // full build
    string fullDoc = "cmd=add,key=0,payload=0 1 2 3,price=0;"
                     "cmd=add,key=1,payload=1 2 3 4,price=1;"
                     "cmd=add,key=2,payload=2 3 4 5,price=2;"
                     "cmd=add,key=3,payload=3 4 5 6,price=3;"
                     "cmd=add,key=4,payload=4 5 6 7,price=4;"
                     "cmd=add,key=5,payload=5 6 7 8,price=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:0", "payload=0 1 2 3,price=0"));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    Pool pool;
    AuxTableReaderTyped<MultiValueType<float> >* auxReader =
        AuxTableReaderCreator::Create<MultiValueType<float> >(reader, "payload", &pool);
    ASSERT_TRUE(auxReader);

    CheckValue(auxReader, "0", "0 1 2 3");
    CheckValue(auxReader, "1", "1 2 3 4");
    CheckValue(auxReader, "2", "2 3 4 5");
    CheckValue(auxReader, "5", "5 6 7 8");

    POOL_COMPATIBLE_DELETE_CLASS(&pool, auxReader);
}




IE_NAMESPACE_END(partition);
