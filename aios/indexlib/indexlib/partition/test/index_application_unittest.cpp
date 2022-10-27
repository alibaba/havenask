#include "indexlib/partition/test/index_application_unittest.h"
#include "indexlib/partition/test/fake_online_partition.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexApplicationTest);

IndexApplicationTest::IndexApplicationTest()
{
}

IndexApplicationTest::~IndexApplicationTest()
{
}

void IndexApplicationTest::CaseSetUp()
{
    schemaA.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaA,
            //Field schema
            "a1:string; a2:string; a3:string",
            //index schema
            "a1:string:a1",
            //attribute schema
            "a2;a3",
            //summary schema
            "");
    schemaA->SetSchemaName("tableA");
    schemaB.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaB,
            //Field schema
            "b1:string; b2:string; b3:string",
            //index schema
            "b1:string:b1",
            //attribute schema
            "b2;b3",
            //summary schema
            "");
    schemaB->SetSchemaName("tableB");
    schemaC.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaC,
            //Field schema
            "c1:string; c2:string; c3:string",
            //index schema
            "c1:string:c1",
            //attribute schemc
            "c2;c3",
            //summary schema
            "");
    schemaC->SetSchemaName("tableC");
    schemaD.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaD,
            //Field schema
            "d1:string; d2:string; d3:string;",
            //index schema
            "d1:string:d1; d2:string:d2",
            //attribute schema
            "d3",
            //summary schema
            "");
    schemaD->SetSchemaName("tableD");
    schemaE.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaE,
            //Field schema
            "e1:string; e2:string; e3:string;",
            //index schema
            "e1:string:e1; e2:string:e2",
            //attribute schema
            "e3",
            //summary schema
            "");
    schemaE->SetSchemaName("tableE");
    readerA.reset(new FakeIndexPartitionReaderBase());
    readerB.reset(new FakeIndexPartitionReaderBase());
    readerC.reset(new FakeIndexPartitionReaderBase());
    readerD.reset(new FakeIndexPartitionReaderBase());
    readerE.reset(new FakeIndexPartitionReaderBase());
}

void IndexApplicationTest::CaseTearDown()
{
}

void IndexApplicationTest::AddSub()
{
    IndexPartitionSchemaPtr subSchemaA(new IndexPartitionSchema());
    IndexPartitionSchemaPtr subSchemaB(new IndexPartitionSchema());
    IndexPartitionSchemaPtr subSchemaD(new IndexPartitionSchema());
    FakeIndexPartitionReaderBasePtr subReaderA(new FakeIndexPartitionReaderBase());
    FakeIndexPartitionReaderBasePtr subReaderB(new FakeIndexPartitionReaderBase());
    FakeIndexPartitionReaderBasePtr subReaderD(new FakeIndexPartitionReaderBase());
    PartitionSchemaMaker::MakeSchema(subSchemaA,
            //Field schema
            "sub_a1:string; sub_a2:string; sub_a3:string",
            //index schema
            "sub_a1:string:sub_a1",
            //attribute schema
            "sub_a2;sub_a3",
            //summary schema
            "");
    PartitionSchemaMaker::MakeSchema(subSchemaB,
            //Field schema
            "sub_b1:string; sub_b2:string; sub_b3:string",
            //index schema
            "sub_b1:string:sub_b1",
            //attribute schema
            "sub_b2;sub_b3",
            //summary schema
            "");
    PartitionSchemaMaker::MakeSchema(subSchemaD,
            //Field schema
            "sub_d1:string; sub_d2:string; sub_d3:string",
            //index schema
            "sub_d1:string:sub_d1",
            //attribute schema
            "sub_d2;sub_d3",
            //summary schema
            "");
    subSchemaA->SetSchemaName("subTableA");
    subSchemaB->SetSchemaName("subTableB");
    subSchemaD->SetSchemaName("subTableD");

    schemaA->SetSubIndexPartitionSchema(subSchemaA);
    schemaB->SetSubIndexPartitionSchema(subSchemaB);
    schemaD->SetSubIndexPartitionSchema(subSchemaD);
    subReaderA->SetSchema(subSchemaA);
    subReaderB->SetSchema(subSchemaB);
    subReaderD->SetSchema(subSchemaD);

    FakeIndexPartitionReaderBase *reader = dynamic_cast<FakeIndexPartitionReaderBase *>(readerA.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderA);
    reader = dynamic_cast<FakeIndexPartitionReaderBase *>(readerB.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderB);
    reader = dynamic_cast<FakeIndexPartitionReaderBase *>(readerD.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderD);
}

void IndexApplicationTest::TestAddIndexPartitions()
{
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    IndexApplication app;
    app.mIndexPartitionVec.push_back(partA);
    app.mIndexPartitionVec.push_back(partB);

    ASSERT_TRUE(app.AddIndexPartitionsFromVec());

    ASSERT_EQ((size_t)2, app.mIndexName2IdMap.size());
    ASSERT_EQ((size_t)4, app.mAttrName2IdMap.size());
    ASSERT_EQ((size_t)2, app.mReaderTypeVec.size());

    ASSERT_EQ((uint32_t)0, app.mIndexName2IdMap[make_pair("tableA", "a1")]);
    ASSERT_EQ((uint32_t)1, app.mIndexName2IdMap[make_pair("tableB", "b1")]);

    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap[make_pair("tableA", "a2")]);
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap[make_pair("tableA", "a3")]);
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap[make_pair("tableB", "b2")]);
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap[make_pair("tableB", "b3")]);

    ASSERT_EQ(READER_PRIMARY_MAIN_TYPE, app.mReaderTypeVec[0]);
    ASSERT_EQ(READER_PRIMARY_MAIN_TYPE, app.mReaderTypeVec[1]);
}

void IndexApplicationTest::TestAddIndexPartitionsWithSubDoc()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    IndexApplication app;
    app.mIndexPartitionVec.push_back(partA);
    app.mIndexPartitionVec.push_back(partB);

    ASSERT_TRUE(app.AddIndexPartitionsFromVec());

    ASSERT_EQ((size_t)4, app.mIndexName2IdMap.size());
    ASSERT_EQ((size_t)8, app.mAttrName2IdMap.size());
    ASSERT_EQ((size_t)4, app.mReaderTypeVec.size());

    ASSERT_EQ((uint32_t)0, app.mIndexName2IdMap[make_pair("tableA", "a1")]);
    ASSERT_EQ((uint32_t)1, app.mIndexName2IdMap[make_pair("subTableA", "sub_a1")]);
    ASSERT_EQ((uint32_t)2, app.mIndexName2IdMap[make_pair("tableB", "b1")]);
    ASSERT_EQ((uint32_t)3, app.mIndexName2IdMap[make_pair("subTableB", "sub_b1")]);

    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap[make_pair("tableA", "a2")]);
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap[make_pair("tableA", "a3")]);
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap[make_pair("subTableA", "sub_a2")]);
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap[make_pair("subTableA", "sub_a3")]);
    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap[make_pair("tableB", "b2")]);
    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap[make_pair("tableB", "b3")]);
    ASSERT_EQ((uint32_t)3, app.mAttrName2IdMap[make_pair("subTableB", "sub_b2")]);
    ASSERT_EQ((uint32_t)3, app.mAttrName2IdMap[make_pair("subTableB", "sub_b3")]);

    ASSERT_EQ(READER_PRIMARY_MAIN_TYPE, app.mReaderTypeVec[0]);
    ASSERT_EQ(READER_PRIMARY_SUB_TYPE, app.mReaderTypeVec[1]);
    ASSERT_EQ(READER_PRIMARY_MAIN_TYPE, app.mReaderTypeVec[2]);
    ASSERT_EQ(READER_PRIMARY_SUB_TYPE, app.mReaderTypeVec[3]);
}

void IndexApplicationTest::TestParseJoinRelations()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);
    IndexPartitionPtr partC = partition::FakeOnlinePartition::Create(schemaC, readerC);
    IndexPartitionPtr partD = partition::FakeOnlinePartition::Create(schemaD, readerD);
    IndexPartitionPtr partE = partition::FakeOnlinePartition::Create(schemaE, readerE);

    std::vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);
    indexPartitions.push_back(partC);
    indexPartitions.push_back(partD);
    indexPartitions.push_back(partE);

    IndexApplication app;
    //ASSERT_TRUE(app.AddIndexPartitions(indexPartitions));

    /*************
      join relations:
      A join C on a2
      subTableA join D on sub_a2
      B join E on b2
      subTableD join C on sub_d2 
    **************/
    
    JoinRelationMap joinRelations;
    vector<JoinField> joinFields;
    JoinField joinField1("a2", "tableC", false, false);
    joinFields.push_back(joinField1);
    joinRelations["tableA"] = joinFields;

    joinFields.clear();
    JoinField joinField2("sub_a2", "tableD", false, false);
    joinFields.push_back(joinField2);
    joinRelations["subTableA"] = joinFields;

    joinFields.clear();
    JoinField joinField3("b2", "tableE", false, false);
    joinFields.push_back(joinField3);
    joinRelations["tableB"] = joinFields;

    joinFields.clear();
    JoinField joinField4("sub_d2", "tableC", false, false);
    joinFields.push_back(joinField4);
    joinRelations["subTableD"] = joinFields;

    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));
    ASSERT_EQ((size_t)10, app.mIndexName2IdMap.size());
    ASSERT_EQ((size_t)14, app.mAttrName2IdMap.size());
    ASSERT_EQ((size_t)8, app.mReaderTypeVec.size());

    ReverseJoinRelationMap reverseJoinMap = *(app.mReaderUpdater->GetJoinRelationMap());
    ASSERT_EQ(size_t(3), reverseJoinMap.size());
    ASSERT_EQ(size_t(2), reverseJoinMap["tableC"].size());
    ASSERT_EQ(size_t(1), reverseJoinMap["tableD"].size());
    ASSERT_EQ(size_t(1), reverseJoinMap["tableE"].size());
    
    EXPECT_EQ(joinField1, reverseJoinMap["tableC"]["tableA"]);
    EXPECT_EQ(joinField4, reverseJoinMap["tableC"]["subTableD"]);
    EXPECT_EQ(joinField2, reverseJoinMap["tableD"]["subTableA"]);
    EXPECT_EQ(joinField3, reverseJoinMap["tableE"]["tableB"]);
}

void IndexApplicationTest::TestCreateSnapshot()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);
    IndexPartitionPtr partC = partition::FakeOnlinePartition::Create(schemaC, readerC);
    IndexPartitionPtr partD = partition::FakeOnlinePartition::Create(schemaD, readerD);
    IndexPartitionPtr partE = partition::FakeOnlinePartition::Create(schemaE, readerE);

    std::vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);
    indexPartitions.push_back(partC);
    indexPartitions.push_back(partD);
    indexPartitions.push_back(partE);
    JoinRelationMap joinRelations;
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));
    //ASSERT_TRUE(app.AddIndexPartitions(indexPartitions));
    
    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    vector<IndexPartitionReaderInfo> indexPartReaderInfos =
        snapshot->mIndexPartReaderInfos;
    
    ASSERT_EQ(readerA, indexPartReaderInfos[0].indexPartReader);
    ASSERT_EQ(readerA->GetSubPartitionReader(), indexPartReaderInfos[1].indexPartReader);
    ASSERT_EQ(readerB, indexPartReaderInfos[2].indexPartReader);
    ASSERT_EQ(readerB->GetSubPartitionReader(), indexPartReaderInfos[3].indexPartReader);
    ASSERT_EQ(readerC, indexPartReaderInfos[4].indexPartReader);
    ASSERT_EQ(readerD, indexPartReaderInfos[5].indexPartReader);
    ASSERT_EQ(readerD->GetSubPartitionReader(), indexPartReaderInfos[6].indexPartReader);
    ASSERT_EQ(readerE, indexPartReaderInfos[7].indexPartReader);

    IndexPartitionSchemaPtr sa = app.GetTableSchema("tableA");
    ASSERT_TRUE(sa);
    IndexPartitionSchemaPtr sb = app.GetTableSchema("tableB");
    ASSERT_TRUE(sb);
    IndexPartitionSchemaPtr sf = app.GetTableSchema("tableF");
    ASSERT_FALSE(sf); 
}

void IndexApplicationTest::TestIsLatestSnapshotReader()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    string fullDocString = "cmd=add,string1=hello1,price=4;"
                           "cmd=add,string1=hello2,price=5;";
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 3;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    IndexApplication::IndexPartitionMap partitionMap;
    partitionMap["table"] = psm.GetIndexPartition();
    JoinRelationMap relationMap;
    IndexApplication app;
    app.Init(partitionMap, relationMap);
    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();

    // no rt -> first rt doc, diff reader
    string rtDocStr1= "cmd=add,string1=hello3,price=6,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr1, "pk:hello3", "docid=2,price=6"));
    ASSERT_FALSE(app.IsLatestSnapshotReader(snapshot));

    snapshot = app.CreateSnapshot();
    // second rt doc, not trigger rt dump
    string rtDocStr2= "cmd=add,string1=hello4,price=7,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr2, "pk:hello4", "docid=3,price=7"));
    ASSERT_TRUE(app.IsLatestSnapshotReader(snapshot));
    ASSERT_FALSE(app.UpdateSnapshotReader(snapshot, "noname"));

    //trigger rt dump
    string rtDocStr3= "cmd=add,string1=hello5,price=8,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr3, "pk:hello5", "docid=4,price=8"));
    ASSERT_FALSE(app.IsLatestSnapshotReader(snapshot));

    string rtDocStr4= "cmd=add,string1=hello6,price=8,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr4, "pk:hello6", "docid=5,price=8"));


    auto& reader = snapshot->GetIndexPartitionReader("noname")->GetPrimaryKeyReader();
    ASSERT_EQ(INVALID_DOCID, reader->Lookup("hello6"));

    ASSERT_TRUE(app.UpdateSnapshotReader(snapshot, "noname"));
    ASSERT_TRUE(app.IsLatestSnapshotReader(snapshot));

    auto& reader2 = snapshot->GetIndexPartitionReader("noname")->GetPrimaryKeyReader();
    ASSERT_EQ((docid_t)5, reader2->Lookup("hello6"));
}

void IndexApplicationTest::TestCreateSnapshotForCustomOnlinePartition()
{
    //prepare partition and reader
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionSchemaPtr subSchemaA(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(subSchemaA,
                                     //Field schema
                                     "sub_a1:string; sub_a2:string; sub_a3:string",
                                     //index schema
                                     "sub_a1:string:sub_a1",
                                     //attribute schema
                                     "sub_a2;sub_a3",
                                     //summary schema
                                     "");
    subSchemaA->SetSchemaName("subTableA");
    schemaA->SetSubIndexPartitionSchema(subSchemaA);
    FakeIndexPartitionReaderBasePtr subReaderA(new FakeIndexPartitionReaderBase());
    subReaderA->SetSchema(subSchemaA);
    FakeIndexPartitionReaderBase *reader = dynamic_cast<FakeIndexPartitionReaderBase *>(readerA.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderA);

    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);
    
    partition::IndexPartitionResource resource;
    resource.taskScheduler.reset(new TaskScheduler());
    resource.memoryQuotaController.reset(new MemoryQuotaController(10000));
    IndexPartitionSchemaPtr customSchema(new IndexPartitionSchema);
    customSchema->SetTableType("customized");
    customSchema->SetSchemaName("tableC");
    CustomOnlinePartitionPtr partC(new CustomOnlinePartition("partc", resource));
    partC->mSchema = customSchema;
    IndexPartitionReaderPtr readerC(new CustomOnlinePartitionReader(
                             customSchema, IndexPartitionOptions(), "partc",
                             TableFactoryWrapperPtr(), TableWriterPtr()));
    partC->mReader = DYNAMIC_POINTER_CAST(CustomOnlinePartitionReader, readerC);

    std::vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);
    indexPartitions.push_back(partC);

    JoinRelationMap joinRelations;
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));
    
    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    vector<IndexPartitionReaderInfo> indexPartReaderInfos =
        snapshot->mIndexPartReaderInfos;
    
    ASSERT_EQ(readerA, indexPartReaderInfos[0].indexPartReader);
    ASSERT_EQ(readerA->GetSubPartitionReader(), indexPartReaderInfos[1].indexPartReader);
    ASSERT_EQ(readerB, indexPartReaderInfos[2].indexPartReader);
    ASSERT_EQ(readerC, indexPartReaderInfos[3].indexPartReader);
    

    IndexPartitionSchemaPtr sa = app.GetTableSchema("tableA");
    ASSERT_TRUE(sa);
    IndexPartitionSchemaPtr sb = app.GetTableSchema("tableB");
    ASSERT_TRUE(sb);
    IndexPartitionSchemaPtr sc = app.GetTableSchema("tableC");
    ASSERT_TRUE(sc);

    IndexPartitionSchemaPtr sf = app.GetTableSchema("tableF");
    ASSERT_FALSE(sf); 
}

IE_NAMESPACE_END(partition);

