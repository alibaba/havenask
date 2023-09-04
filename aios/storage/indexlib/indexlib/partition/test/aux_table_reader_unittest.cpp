#include "indexlib/partition/test/aux_table_reader_unittest.h"

#include "indexlib/partition/aux_table_reader_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, AuxTableReaderTest);

AuxTableReaderTest::AuxTableReaderTest() {}

AuxTableReaderTest::~AuxTableReaderTest() {}

void AuxTableReaderTest::CaseSetUp() {}

void AuxTableReaderTest::CaseTearDown() {}

void CheckValue(AuxTableReaderTyped<MultiValueType<float>>* auxReader, const std::string& key,
                const std::string& expectValueStr)
{
    MultiValueType<float> value;
    ASSERT_TRUE(auxReader->GetValue(key, value));

    vector<float> expectValues;
    StringUtil::fromString(expectValueStr, expectValues, " ");

    ASSERT_EQ(value.size(), expectValues.size());
    for (size_t i = 0; i < value.size(); i++) {
        ASSERT_EQ(expectValues[i], value[i]);
    }
}

void AuxTableReaderTest::TestReadFixLengthFloatFromKVTable()
{
    string field = "nid:uint64;score:float:true:false:int8#127:4";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "nid", "score");

    string docString = "cmd=add,nid=1,score=1 2 2 3;"
                       "cmd=add,nid=2,score=2 -6;"
                       "cmd=add,nid=3,score=-1 0 -2 4 5;";

    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH() + "/int8"));
    IndexPartitionResource& resource = psm.GetIndexPartitionResource();
    SearchCachePtr searchCache(new SearchCache(40960, resource.memoryQuotaController, resource.taskScheduler, NULL, 6));
    resource.searchCache.reset(new SearchCachePartitionWrapper(searchCache, ""));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "1", "score=1 2 2 3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "2", "score=2 -6 0 0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "3", "score=-1 0 -2 4"));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader.get());

    Pool pool;
    AuxTableReaderTyped<MultiValueType<float>>* auxReader =
        AuxTableReaderCreator::Create<MultiValueType<float>>(reader, "score", &pool);
    ASSERT_TRUE(auxReader);

    CheckValue(auxReader, "1", "1 2 2 3");
    CheckValue(auxReader, "2", "2 -6 0 0");
    CheckValue(auxReader, "3", "-1 0 -2 4");

    POOL_COMPATIBLE_DELETE_CLASS(&pool, auxReader);
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
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));

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
    AuxTableReaderTyped<MultiValueType<float>>* auxReader =
        AuxTableReaderCreator::Create<MultiValueType<float>>(reader, "payload", &pool);
    ASSERT_TRUE(auxReader);

    CheckValue(auxReader, "0", "0 1 2 3");
    CheckValue(auxReader, "1", "1 2 3 4");
    CheckValue(auxReader, "2", "2 3 4 5");
    CheckValue(auxReader, "5", "5 6 7 8");

    POOL_COMPATIBLE_DELETE_CLASS(&pool, auxReader);
}

void AuxTableReaderTest::TestReadNullValueFromIndexTable()
{
    string field = "name:string;single_value:uint32;multi_value:int8:true:true";
    string index = "pk:primarykey64:name";
    string attributes = "single_value;multi_value";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attributes, "");
    SchemaMaker::EnableNullFields(schema, "single_value;multi_value");

    string fullDocs = "cmd=add,name=doc1;";
    IndexPartitionOptions options;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "pk:doc1", "docid=0"));
    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(reader);

    Pool pool;
    {
        // check single value
        AuxTableReaderTyped<uint32_t>* auxReader =
            AuxTableReaderCreator::Create<uint32_t>(reader, "single_value", &pool);
        ASSERT_TRUE(auxReader);

        uint32_t value;
        bool isNull = false;
        ASSERT_TRUE(auxReader->GetValue("doc1", value, isNull));
        ASSERT_TRUE(isNull);
        POOL_COMPATIBLE_DELETE_CLASS(&pool, auxReader);
    }

    {
        // check multi value
        AuxTableReaderTyped<MultiValueType<int8_t>>* auxReader =
            AuxTableReaderCreator::Create<MultiValueType<int8_t>>(reader, "multi_value", &pool);
        ASSERT_TRUE(auxReader);

        MultiValueType<int8_t> value;
        bool isNull = false;
        ASSERT_TRUE(auxReader->GetValue("doc1", value, isNull));
        ASSERT_TRUE(isNull);
        ASSERT_TRUE(value.isNull());
        POOL_COMPATIBLE_DELETE_CLASS(&pool, auxReader);
    }
}
}} // namespace indexlib::partition
