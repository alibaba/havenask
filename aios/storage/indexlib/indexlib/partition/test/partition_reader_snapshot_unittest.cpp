#include "indexlib/partition/test/partition_reader_snapshot_unittest.h"

#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionReaderSnapshotTest);

PartitionReaderSnapshotTest::PartitionReaderSnapshotTest() {}

PartitionReaderSnapshotTest::~PartitionReaderSnapshotTest() {}

void PartitionReaderSnapshotTest::CaseSetUp() {}

void PartitionReaderSnapshotTest::CaseTearDown() {}

void PartitionReaderSnapshotTest::TestCreateAuxTableFromSingleValueKVTable()
{
    string field = "key:uint64;value:float:false;";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    schema->SetEnableTTL(false);
    schema->SetSchemaName("TestCreateAuxTableFromSingleValueKVTable_schema");

    string docString = "cmd=add,key=1,value=1.4;"
                       "cmd=add,key=2,value=2.4";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, IndexPartitionOptions(), GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1.4"));

    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({psm.GetIndexPartition()}, JoinRelationMap()));

    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);

    autil::mem_pool::Pool pool;
    AuxTableReaderTyped<float>* auxTableReader = snapshot->CreateAuxTableReader<float>("value", "", &pool);
    ASSERT_TRUE(auxTableReader != nullptr);
    ASSERT_EQ(tt_kv, auxTableReader->GetTableType());
    float value;
    ASSERT_TRUE(auxTableReader->GetValue("1", value));
    EXPECT_EQ(1.4f, value);
    ASSERT_TRUE(auxTableReader->GetValue("2", value));
    EXPECT_EQ(2.4f, value);
    ASSERT_FALSE(auxTableReader->GetValue("3", value));
    POOL_DELETE_CLASS(auxTableReader);

    auxTableReader =
        snapshot->CreateAuxTableReader<float>("value", "TestCreateAuxTableFromSingleValueKVTable_schema", &pool);
    ASSERT_TRUE(auxTableReader);
    POOL_DELETE_CLASS(auxTableReader);
}

void PartitionReaderSnapshotTest::TestCreateAuxTableFromMultiValueKVTable()
{
    string field = "key:uint64;value:float:true;";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    schema->SetEnableTTL(false);
    schema->SetSchemaName("TestCreateAuxTableFromMultiValueKVTable_schema");

    string docString = "cmd=add,key=1,value=1.4 1.5 1.6;"
                       "cmd=add,key=2,value=2.4 2.5";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, IndexPartitionOptions(), GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({psm.GetIndexPartition()}, JoinRelationMap()));

    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);

    autil::mem_pool::Pool pool;
    AuxTableReaderTyped<MultiFloat>* auxTableReader = snapshot->CreateAuxTableReader<MultiFloat>("value", "", &pool);
    ASSERT_TRUE(auxTableReader != nullptr);
    ASSERT_EQ(tt_kv, auxTableReader->GetTableType());
    MultiFloat value;
    ASSERT_TRUE(auxTableReader->GetValue("1", value));
    ASSERT_EQ(size_t(3), value.size());
    EXPECT_EQ(1.4f, value[0]);
    EXPECT_EQ(1.5f, value[1]);
    EXPECT_EQ(1.6f, value[2]);

    ASSERT_TRUE(auxTableReader->GetValue("2", value));
    ASSERT_EQ(size_t(2), value.size());
    EXPECT_EQ(2.4f, value[0]);
    EXPECT_EQ(2.5f, value[1]);

    ASSERT_FALSE(auxTableReader->GetValue("3", value));
    POOL_DELETE_CLASS(auxTableReader);
}

void PartitionReaderSnapshotTest::TestQueryAuxTableByHashKVTable()
{
    string field = "key:uint64;value:float:false;";
    auto schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    schema->SetEnableTTL(false);
    schema->SetSchemaName("TestCreateAuxTableFromSingleValueKVTable_schema");

    string docString = "cmd=add,key=1,value=1.4;"
                       "cmd=add,key=2,value=2.4";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, IndexPartitionOptions(), GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "1", "value=1.4"));

    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({psm.GetIndexPartition()}, JoinRelationMap()));

    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);

    autil::mem_pool::Pool pool;
    AuxTableReaderTyped<float>* auxTableReader = snapshot->CreateAuxTableReader<float>("value", "", &pool);
    ASSERT_TRUE(auxTableReader != nullptr);
    AuxTableReaderHasher<> hasher;
    hasher = auxTableReader->GetHasher();
    ASSERT_EQ(tt_kv, auxTableReader->GetTableType());
    float value;
    uint64_t key;
    ASSERT_TRUE(hasher("1", 1, key));
    ASSERT_TRUE(auxTableReader->GetValue(key, value));
    EXPECT_EQ(1.4f, value);
    ASSERT_TRUE(hasher("2", 1, key));
    ASSERT_TRUE(auxTableReader->GetValue(key, value));
    EXPECT_EQ(2.4f, value);
    ASSERT_TRUE(hasher("3", 1, key));
    ASSERT_FALSE(auxTableReader->GetValue(key, value));
    POOL_DELETE_CLASS(auxTableReader);

    auxTableReader =
        snapshot->CreateAuxTableReader<float>("value", "TestCreateAuxTableFromSingleValueKVTable_schema", &pool);
    ASSERT_TRUE(auxTableReader);
    POOL_DELETE_CLASS(auxTableReader);
}

void PartitionReaderSnapshotTest::TestQueryAuxTableByHashIndexTable()
{
    string field = "pk:uint64:pk;int:int32";
    string index = "pk:primarykey64:pk;";
    string attr = "int;";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
    ASSERT_TRUE(schema);
    schema->SetSchemaName("TestQueryAuxTableByHashIndexTable_schema");
    auto pkConfig = schema->GetIndexSchema()->GetPrimaryKeyIndexConfig();
    auto pkIndexConfig = DYNAMIC_POINTER_CAST(PrimaryKeyIndexConfig, pkConfig);
    pkIndexConfig->SetPrimaryKeyHashType(index::pk_number_hash);

    string docString = "cmd=add,pk=1,int=1;"
                       "cmd=add,pk=2,int=2;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, IndexPartitionOptions(), GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:1", "int=1"));

    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({psm.GetIndexPartition()}, JoinRelationMap()));

    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);

    autil::mem_pool::Pool pool;
    auto* auxTableReader = snapshot->CreateAuxTableReader<int>("int", "", &pool);

    ASSERT_TRUE(auxTableReader != nullptr);
    AuxTableReaderHasher<> hasher;
    hasher = auxTableReader->GetHasher();
    ASSERT_EQ(tt_index, auxTableReader->GetTableType());
    int value;
    uint64_t key;
    ASSERT_TRUE(hasher("1", 1, key));
    ASSERT_TRUE(auxTableReader->GetValue(key, value)) << key;
    EXPECT_EQ(1, value);
    ASSERT_TRUE(hasher("2", 1, key));
    ASSERT_TRUE(auxTableReader->GetValue(key, value));
    EXPECT_EQ(2, value);
    POOL_DELETE_CLASS(auxTableReader);

    auxTableReader = snapshot->CreateAuxTableReader<int>("int", "TestQueryAuxTableByHashIndexTable_schema", &pool);
    ASSERT_TRUE(auxTableReader);
    POOL_DELETE_CLASS(auxTableReader);
}
}} // namespace indexlib::partition
