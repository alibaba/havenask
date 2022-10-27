#include "indexlib/partition/test/partition_reader_snapshot_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_application.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionReaderSnapshotTest);

PartitionReaderSnapshotTest::PartitionReaderSnapshotTest()
{
}

PartitionReaderSnapshotTest::~PartitionReaderSnapshotTest()
{
}

void PartitionReaderSnapshotTest::CaseSetUp()
{
}

void PartitionReaderSnapshotTest::CaseTearDown()
{
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
    pkIndexConfig->SetPrimaryKeyHashType(pk_number_hash);

    string docString = "cmd=add,pk=1,int=1;"
                       "cmd=add,pk=2,int=2;";

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, IndexPartitionOptions(), GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "pk:1", "int=1"));

    IndexApplication indexApp;
    ASSERT_TRUE(indexApp.Init({psm.GetIndexPartition()}, JoinRelationMap()));

    auto snapshot = indexApp.CreateSnapshot();
    ASSERT_TRUE(snapshot);

    autil::mem_pool::Pool pool;
    auto *auxTableReader = snapshot->CreateAuxTableReader<int>("int", "", &pool);

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

    auxTableReader = snapshot->CreateAuxTableReader<int>(
            "int", "TestQueryAuxTableByHashIndexTable_schema", &pool);
    ASSERT_TRUE(auxTableReader);
    POOL_DELETE_CLASS(auxTableReader);
}

IE_NAMESPACE_END(partition);
