#include "indexlib/partition/test/index_application_unittest.h"

#include "indexlib/base/Status.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/partition/test/fake_online_partition.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::table;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexApplicationTest);

IndexApplicationTest::IndexApplicationTest() {}

IndexApplicationTest::~IndexApplicationTest() {}

void IndexApplicationTest::CaseSetUp()
{
    schemaA.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaA,
                                     // Field schema
                                     "a1:string; a2:string; a3:string",
                                     // index schema
                                     "a1:string:a1",
                                     // attribute schema
                                     "a2;a3",
                                     // summary schema
                                     "");
    schemaA->SetSchemaName("tableA");
    schemaB.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaB,
                                     // Field schema
                                     "b1:string; b2:string; b3:string",
                                     // index schema
                                     "b1:string:b1",
                                     // attribute schema
                                     "b2;b3",
                                     // summary schema
                                     "");
    schemaB->SetSchemaName("tableB");
    schemaC.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaC,
                                     // Field schema
                                     "c1:string; c2:string; c3:string",
                                     // index schema
                                     "c1:string:c1",
                                     // attribute schemc
                                     "c2;c3",
                                     // summary schema
                                     "");
    schemaC->SetSchemaName("tableC");
    schemaD.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaD,
                                     // Field schema
                                     "d1:string; d2:string; d3:string;",
                                     // index schema
                                     "d1:string:d1; d2:string:d2",
                                     // attribute schema
                                     "d3",
                                     // summary schema
                                     "");
    schemaD->SetSchemaName("tableD");
    schemaE.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaE,
                                     // Field schema
                                     "e1:string; e2:string; e3:string;",
                                     // index schema
                                     "e1:string:e1; e2:string:e2",
                                     // attribute schema
                                     "e3",
                                     // summary schema
                                     "");
    schemaE->SetSchemaName("tableE");
    readerA.reset(new FakeIndexPartitionReaderBase());
    readerB.reset(new FakeIndexPartitionReaderBase());
    readerC.reset(new FakeIndexPartitionReaderBase());
    readerD.reset(new FakeIndexPartitionReaderBase());
    readerE.reset(new FakeIndexPartitionReaderBase());

    // for tablet
    _executor.reset(new future_lite::executors::SimpleExecutor(5));
    _taskScheduler.reset(new future_lite::TaskScheduler(_executor.get()));
}

void IndexApplicationTest::CaseTearDown() {}

void IndexApplicationTest::AddSub()
{
    IndexPartitionSchemaPtr subSchemaA(new IndexPartitionSchema());
    IndexPartitionSchemaPtr subSchemaB(new IndexPartitionSchema());
    IndexPartitionSchemaPtr subSchemaD(new IndexPartitionSchema());
    FakeIndexPartitionReaderBasePtr subReaderA(new FakeIndexPartitionReaderBase());
    FakeIndexPartitionReaderBasePtr subReaderB(new FakeIndexPartitionReaderBase());
    FakeIndexPartitionReaderBasePtr subReaderD(new FakeIndexPartitionReaderBase());
    PartitionSchemaMaker::MakeSchema(subSchemaA,
                                     // Field schema
                                     "sub_a1:string; sub_a2:string; sub_a3:string",
                                     // index schema
                                     "sub_a1:string:sub_a1",
                                     // attribute schema
                                     "sub_a2;sub_a3",
                                     // summary schema
                                     "");
    PartitionSchemaMaker::MakeSchema(subSchemaB,
                                     // Field schema
                                     "sub_b1:string; sub_b2:string; sub_b3:string",
                                     // index schema
                                     "sub_b1:string:sub_b1",
                                     // attribute schema
                                     "sub_b2;sub_b3",
                                     // summary schema
                                     "");
    PartitionSchemaMaker::MakeSchema(subSchemaD,
                                     // Field schema
                                     "sub_d1:string; sub_d2:string; sub_d3:string",
                                     // index schema
                                     "sub_d1:string:sub_d1",
                                     // attribute schema
                                     "sub_d2;sub_d3",
                                     // summary schema
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

    FakeIndexPartitionReaderBase* reader = dynamic_cast<FakeIndexPartitionReaderBase*>(readerA.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderA);
    reader = dynamic_cast<FakeIndexPartitionReaderBase*>(readerB.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderB);
    reader = dynamic_cast<FakeIndexPartitionReaderBase*>(readerD.get());
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

    ASSERT_EQ((size_t)2, app.mIndexName2IdMap.Size());
    ASSERT_EQ((size_t)4, app.mAttrName2IdMap.Size());
    ASSERT_EQ((size_t)2, app.mReaderTypeVec.size());

    ASSERT_EQ((uint32_t)0, app.mIndexName2IdMap.FindValueWithDefault("tableA", "a1"));
    ASSERT_EQ((uint32_t)1, app.mIndexName2IdMap.FindValueWithDefault("tableB", "b1"));

    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a2"));
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a3"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b2"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b3"));

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

    ASSERT_EQ((size_t)4, app.mIndexName2IdMap.Size());
    ASSERT_EQ((size_t)8, app.mAttrName2IdMap.Size());
    ASSERT_EQ((size_t)4, app.mReaderTypeVec.size());

    ASSERT_EQ((uint32_t)0, app.mIndexName2IdMap.FindValueWithDefault("tableA", "a1"));
    ASSERT_EQ((uint32_t)1, app.mIndexName2IdMap.FindValueWithDefault("subTableA", "sub_a1"));
    ASSERT_EQ((uint32_t)2, app.mIndexName2IdMap.FindValueWithDefault("tableB", "b1"));
    ASSERT_EQ((uint32_t)3, app.mIndexName2IdMap.FindValueWithDefault("subTableB", "sub_b1"));

    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a2"));
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a3"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("subTableA", "sub_a2"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("subTableA", "sub_a3"));
    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b2"));
    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b3"));
    ASSERT_EQ((uint32_t)3, app.mAttrName2IdMap.FindValueWithDefault("subTableB", "sub_b2"));
    ASSERT_EQ((uint32_t)3, app.mAttrName2IdMap.FindValueWithDefault("subTableB", "sub_b3"));

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
    // ASSERT_TRUE(app.AddIndexPartitions(indexPartitions));

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
    ASSERT_EQ((size_t)10, app.mIndexName2IdMap.Size());
    ASSERT_EQ((size_t)14, app.mAttrName2IdMap.Size());
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
    // ASSERT_TRUE(app.AddIndexPartitions(indexPartitions));

    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    ASSERT_EQ(0, snapshot->getTableMainSubIdxMap().size());
    vector<IndexPartitionReaderInfo> indexPartReaderInfos = snapshot->mIndexPartReaderInfos;

    ASSERT_EQ(readerA, indexPartReaderInfos[0].indexPartReader);
    ASSERT_EQ(readerA->GetSubPartitionReader(), indexPartReaderInfos[1].indexPartReader);
    ASSERT_EQ(readerB, indexPartReaderInfos[2].indexPartReader);
    ASSERT_EQ(readerB->GetSubPartitionReader(), indexPartReaderInfos[3].indexPartReader);
    ASSERT_EQ(readerC, indexPartReaderInfos[4].indexPartReader);
    ASSERT_EQ(readerD, indexPartReaderInfos[5].indexPartReader);
    ASSERT_EQ(readerD->GetSubPartitionReader(), indexPartReaderInfos[6].indexPartReader);
    ASSERT_EQ(readerE, indexPartReaderInfos[7].indexPartReader);

    auto sa = app.GetTableSchema("tableA");
    ASSERT_TRUE(sa);
    auto sb = app.GetTableSchema("tableB");
    ASSERT_TRUE(sb);
    auto sf = app.GetTableSchema("tableF");
    ASSERT_FALSE(sf);
}

void IndexApplicationTest::TestCreateSnapshotWithCache()
{
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    std::vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    JoinRelationMap joinRelations;
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));

    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot("", 100 * 1000);
    ASSERT_EQ(1, snapshot->getTableMainSubIdxMap().size());

    vector<IndexPartitionReaderInfo> indexPartReaderInfos = snapshot->mIndexPartReaderInfos;
    ASSERT_EQ(readerA, indexPartReaderInfos[0].indexPartReader);
    PartitionReaderSnapshotPtr snapshot2 = app.CreateSnapshot("", 100 * 1000);
    ASSERT_TRUE(snapshot2.get() == snapshot.get());
    usleep(2000 * 1000);
    PartitionReaderSnapshotPtr snapshot3 = app.CreateSnapshot("", 100 * 1000);
    ASSERT_TRUE(snapshot2.get() == snapshot.get());
    ASSERT_TRUE(snapshot3.get() != snapshot.get());
    app.updateCallBack();
    PartitionReaderSnapshotPtr snapshot4 = app.CreateSnapshot("", 100 * 1000);
    ASSERT_EQ(1, snapshot4->getTableMainSubIdxMap().size());
    ASSERT_TRUE(snapshot3.get() != snapshot4.get());
}

void IndexApplicationTest::TestCreateSnapshotWithMainSubIdx()
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

    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot("", 100 * 1000);
    auto& mainSubIndexMap = snapshot->getTableMainSubIdxMap();
    ASSERT_EQ(5, mainSubIndexMap.size());
    auto iter = mainSubIndexMap.find("tableA");
    ASSERT_TRUE(iter != mainSubIndexMap.end());
    TableMainSubIdx mainSubIndex = iter->second;
    ASSERT_TRUE(mainSubIndex.attrName2IdMap.find("a1") == mainSubIndex.attrName2IdMap.end());
    ASSERT_EQ(0, mainSubIndex.attrName2IdMap["a2"]);
    ASSERT_EQ(1, mainSubIndex.attrName2IdMap["sub_a2"]);
    ASSERT_TRUE(mainSubIndex.indexName2IdMap.find("a2") == mainSubIndex.indexName2IdMap.end());
    ASSERT_EQ(0, mainSubIndex.indexName2IdMap["a1"]);
    ASSERT_EQ(1, mainSubIndex.indexName2IdMap["sub_a1"]);
    ASSERT_EQ(readerA, mainSubIndex.indexReaderVec[0]);
    ASSERT_EQ(readerA->GetSubPartitionReader(), mainSubIndex.indexReaderVec[1]);
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
    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.GetBuildConfig().maxDocCount = 3;

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocString, "", ""));
    IndexApplication::IndexPartitionMap partitionMap;
    partitionMap["table"] = psm.GetIndexPartition();
    JoinRelationMap relationMap;
    IndexApplication app;
    app.Init(partitionMap, relationMap);
    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();

    // no rt -> first rt doc, diff reader
    string rtDocStr1 = "cmd=add,string1=hello3,price=6,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr1, "pk:hello3", "docid=2,price=6"));
    ASSERT_FALSE(app.IsLatestSnapshotReader(snapshot));

    snapshot = app.CreateSnapshot();
    // second rt doc, not trigger rt dump
    string rtDocStr2 = "cmd=add,string1=hello4,price=7,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr2, "pk:hello4", "docid=3,price=7"));
    ASSERT_TRUE(app.IsLatestSnapshotReader(snapshot));
    ASSERT_FALSE(app.UpdateSnapshotReader(snapshot, "noname"));

    // trigger rt dump
    string rtDocStr3 = "cmd=add,string1=hello5,price=8,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocStr3, "pk:hello5", "docid=4,price=8"));
    ASSERT_FALSE(app.IsLatestSnapshotReader(snapshot));

    string rtDocStr4 = "cmd=add,string1=hello6,price=8,ts=1;";
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
    // prepare partition and reader
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionSchemaPtr subSchemaA(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(subSchemaA,
                                     // Field schema
                                     "sub_a1:string; sub_a2:string; sub_a3:string",
                                     // index schema
                                     "sub_a1:string:sub_a1",
                                     // attribute schema
                                     "sub_a2;sub_a3",
                                     // summary schema
                                     "");
    subSchemaA->SetSchemaName("subTableA");
    schemaA->SetSubIndexPartitionSchema(subSchemaA);
    FakeIndexPartitionReaderBasePtr subReaderA(new FakeIndexPartitionReaderBase());
    subReaderA->SetSchema(subSchemaA);
    FakeIndexPartitionReaderBase* reader = dynamic_cast<FakeIndexPartitionReaderBase*>(readerA.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderA);

    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    partition::IndexPartitionResource resource;
    resource.taskScheduler.reset(new TaskScheduler());
    resource.memoryQuotaController.reset(new MemoryQuotaController(10000));
    resource.partitionName = "partc";
    IndexPartitionSchemaPtr customSchema(new IndexPartitionSchema);
    customSchema->SetTableType("customized");
    customSchema->SetSchemaName("tableC");
    CustomOnlinePartitionPtr partC(new CustomOnlinePartition(resource));
    partC->mSchema = customSchema;
    IndexPartitionReaderPtr readerC(new CustomOnlinePartitionReader(customSchema, IndexPartitionOptions(), "partc",
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
    vector<IndexPartitionReaderInfo> indexPartReaderInfos = snapshot->mIndexPartReaderInfos;

    ASSERT_EQ(readerA, indexPartReaderInfos[0].indexPartReader);
    ASSERT_EQ(readerA->GetSubPartitionReader(), indexPartReaderInfos[1].indexPartReader);
    ASSERT_EQ(readerB, indexPartReaderInfos[2].indexPartReader);
    ASSERT_EQ(readerC, indexPartReaderInfos[3].indexPartReader);

    auto sa = app.GetTableSchema("tableA");
    ASSERT_TRUE(sa);
    auto sb = app.GetTableSchema("tableB");
    ASSERT_TRUE(sb);
    auto sc = app.GetTableSchema("tableC");
    ASSERT_TRUE(sc);

    auto sf = app.GetTableSchema("tableF");
    ASSERT_FALSE(sf);
}

std::shared_ptr<indexlibv2::config::TabletSchema>
IndexApplicationTest::GetDefaultTabletSchema(const std::string& schemaName, const std::string& fieldStr,
                                             const std::string& indexStr, const std::string& attrStr)
{
    auto oldSchema = std::make_shared<IndexPartitionSchema>();
    PartitionSchemaMaker::MakeSchema(oldSchema, fieldStr, indexStr, attrStr,
                                     // summary schema
                                     "");
    oldSchema->SetSchemaName(schemaName);
    auto jsonStr = ToJsonString(*oldSchema);
    return indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
}

std::shared_ptr<indexlibv2::framework::Tablet>
IndexApplicationTest::GetDefaultTablet(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
{
    kmonitor::MetricsTags metricsTags;
    indexlibv2::framework::TabletResource resource;
    resource.metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
    resource.dumpExecutor = _executor.get();
    resource.taskScheduler = _taskScheduler.get();

    auto tabletOptions = std::make_unique<indexlibv2::config::TabletOptions>();
    std::string localRoot = GET_TEMP_DATA_PATH();
    indexlibv2::framework::IndexRoot indexRoot(localRoot, /*remoteRoot*/ localRoot);
    auto tablet = std::make_shared<indexlibv2::framework::Tablet>(resource);
    Status st = tablet->Open(indexRoot, schema, std::move(tabletOptions), /*INVALID_VERSIONID*/ -1);
    if (!st.IsOK()) {
        return nullptr;
    }
    return tablet;
}

void IndexApplicationTest::TestUpdateExpiredSnapshotReader()
{
    setenv("background_update_cache_snapshot_time_us", "1000000", 1000000);
    const std::string schemaA("tabletSchemaA");
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a1:string; a2:string; a3:string",
                                         // index schema
                                         "pk:primarykey64:a1",
                                         // attribute schema
                                         "a2;a3");
    auto tablet = GetDefaultTablet(schema);
    ASSERT_TRUE(schema != nullptr);
    ASSERT_TRUE(tablet != nullptr);

    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    IndexApplication app;
    ASSERT_TRUE(app.InitByTablet(tabletMap));
    auto readerSnapshot = app.CreateSnapshot("", 500 * 1000);
    ASSERT_TRUE(readerSnapshot);
    ASSERT_TRUE(app.mLastPartitionReaderSnapshot);
    ASSERT_TRUE(readerSnapshot == app.mLastPartitionReaderSnapshot);

    usleep(3 * 500 * 1000);

    ASSERT_TRUE(app.mLastPartitionReaderSnapshot);
    ASSERT_TRUE(readerSnapshot != app.mLastPartitionReaderSnapshot);
}

void IndexApplicationTest::TestInitByTablet()
{
    const std::string schemaA("tabletSchemaA");
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a1:string; a2:string; a3:string",
                                         // index schema
                                         "pk:primarykey64:a1",
                                         // attribute schema
                                         "a2;a3");
    auto tablet = GetDefaultTablet(schema);
    ASSERT_TRUE(schema != nullptr);
    ASSERT_TRUE(tablet != nullptr);

    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    IndexApplication app;
    ASSERT_TRUE(app.InitByTablet(tabletMap));
    ASSERT_TRUE(app._hasTablet);
    ASSERT_EQ(1, app._tablets.size());
    ASSERT_EQ(2, app.mAttrName2IdMap.Size());
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a2"));
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a3"));
    ASSERT_EQ(1, app.mIndexName2IdMap.Size());
    ASSERT_TRUE(app.mIndexName2IdMap.Exist(schemaA, "pk"));
}

void IndexApplicationTest::TestInitByTabletFailed()
{
    const std::string schemaA("tabletSchemaA");
    const std::string schemaB("tabletSchemaB");
    auto makeTablet = [&](const std::string& schemaName) -> std::shared_ptr<indexlibv2::framework::ITablet> {
        auto schema = GetDefaultTabletSchema(schemaName, // Field schema
                                             "a1:string; a2:string; a3:string",
                                             // index schema
                                             "pk:primarykey64:a1",
                                             // attribute schema
                                             "a2;a3");
        return GetDefaultTablet(schema);
    };

    auto tabletA = makeTablet(schemaA);
    auto tabletB = makeTablet(schemaA);

    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tabletA;
    tabletMap[schemaB] = tabletB;

    IndexApplication app;
    ASSERT_FALSE(app.InitByTablet(tabletMap));
}

void IndexApplicationTest::TestCreateSnapshot4SingleTablet()
{
    const std::string schemaA("tabletSchemaA");
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a1:string; a2:string; a3:string:true",
                                         // index schema
                                         "pk:primarykey64:a1",
                                         // attribute schema
                                         "a2;a3");
    auto tablet = GetDefaultTablet(schema);
    ASSERT_TRUE(schema != nullptr);
    ASSERT_TRUE(tablet != nullptr);

    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    IndexApplication app;
    ASSERT_TRUE(app.InitByTablet(tabletMap));
    auto snapshot = app.CreateSnapshot();
    ASSERT_TRUE(snapshot != nullptr);
    ASSERT_EQ(1, snapshot->_tabletReaderInfos.size());
    ASSERT_EQ(1, snapshot->mTableName2Idx.size());
    ASSERT_TRUE(snapshot->mTableName2Idx.find(schemaA) != snapshot->mTableName2Idx.end());
    ASSERT_EQ(1, snapshot->mIndexName2Idx.size());
    ASSERT_TRUE(snapshot->mIndexName2Idx.find("pk") != snapshot->mIndexName2Idx.end());
    ASSERT_EQ(2, snapshot->mAttributeName2Idx.size());
    ASSERT_TRUE(snapshot->mAttributeName2Idx.find("a2") != snapshot->mAttributeName2Idx.end());
    ASSERT_TRUE(snapshot->mAttributeName2Idx.find("a3") != snapshot->mAttributeName2Idx.end());

    auto tabletReader = snapshot->GetTabletReader(schemaA);
    ASSERT_TRUE(tabletReader != nullptr);
    tabletReader = snapshot->GetTabletReader(/*idx*/ 0);
    ASSERT_TRUE(tabletReader != nullptr);

    auto normalTabletReader =
        std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tablet->GetTabletReader());
    AttributeReaderInfoV2 attributeReaderInfo2;
    AttributeReaderInfoV2 attributeReaderInfo3;
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a2", schemaA, attributeReaderInfo2));
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a3", schemaA, attributeReaderInfo3));
    ASSERT_EQ(attributeReaderInfo2.indexPartReaderType, attributeReaderInfo3.indexPartReaderType);

    auto attrReader2 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a2");
    auto attrReader3 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a3");
    ASSERT_EQ(attrReader2, attributeReaderInfo2.attrReader);
    ASSERT_EQ(attrReader3, attributeReaderInfo3.attrReader);
    ASSERT_FALSE(attrReader2->IsMultiValue());
    ASSERT_TRUE(attrReader3->IsMultiValue());
}

void IndexApplicationTest::TestAddIndexPartitionsAndTablets()
{
    // create index parttions
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    IndexApplication app;
    app.mIndexPartitionVec.push_back(partA);
    app.mIndexPartitionVec.push_back(partB);
    ASSERT_TRUE(app.AddIndexPartitionsFromVec());
    // create tablet
    const std::string schemaA("tabletSchemaA");
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a1:string; a2:string; a3:string",
                                         // index schema
                                         "pk:primarykey64:a1",
                                         // attribute schema
                                         "a2;a3");
    auto tablet = GetDefaultTablet(schema);
    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    ASSERT_TRUE(app.InitByTablet(tabletMap));
    ASSERT_TRUE(app._hasTablet);
    ASSERT_EQ(1, app._tablets.size());
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a2"));
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a3"));
    ASSERT_TRUE(app.mIndexName2IdMap.Exist(schemaA, "pk"));

    ASSERT_EQ((size_t)3, app.mIndexName2IdMap.Size());
    ASSERT_EQ((size_t)6, app.mAttrName2IdMap.Size());
    ASSERT_EQ((size_t)3, app.mReaderTypeVec.size());

    ASSERT_EQ((uint32_t)0, app.mIndexName2IdMap.FindValueWithDefault("tableA", "a1"));
    ASSERT_EQ((uint32_t)1, app.mIndexName2IdMap.FindValueWithDefault("tableB", "b1"));
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a2"));
    ASSERT_EQ((uint32_t)0, app.mAttrName2IdMap.FindValueWithDefault("tableA", "a3"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b2"));
    ASSERT_EQ((uint32_t)1, app.mAttrName2IdMap.FindValueWithDefault("tableB", "b3"));

    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap.FindValueWithDefault(schemaA, "a2"));
    ASSERT_EQ((uint32_t)2, app.mAttrName2IdMap.FindValueWithDefault(schemaA, "a3"));
    ASSERT_EQ((uint32_t)2, app.mIndexName2IdMap.FindValueWithDefault(schemaA, "pk"));
}

void IndexApplicationTest::TestCreateSnapshot4PartitionAndTablet()
{
    // create index parttions
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    std::vector<partition::IndexPartitionPtr> indexPartitionVec;
    indexPartitionVec.push_back(partA);
    indexPartitionVec.push_back(partB);
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitionVec));
    // create tablet
    const std::string schemaA("tabletSchemaA");
    // WARN: duplicate fieldName will be erase
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a11:string; a22:string; a33:string",
                                         // index schema
                                         "pk:primarykey64:a11",
                                         // attribute schema
                                         "a22;a33");
    auto tablet = GetDefaultTablet(schema);
    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    ASSERT_TRUE(app.InitByTablet(tabletMap));
    auto snapshot = app.CreateSnapshot();
    ASSERT_TRUE(snapshot != nullptr);
    ASSERT_EQ(1, snapshot->_tabletReaderInfos.size());
    ASSERT_EQ(2, snapshot->mIndexPartReaderInfos.size());
    ASSERT_EQ(3, snapshot->mTableName2Idx.size());
    ASSERT_TRUE(snapshot->mTableName2Idx.find(schemaA) != snapshot->mTableName2Idx.end());
    ASSERT_EQ(3, snapshot->mIndexName2Idx.size());
    ASSERT_TRUE(snapshot->mIndexName2Idx.find("pk") != snapshot->mIndexName2Idx.end());
    ASSERT_EQ(6, snapshot->mAttributeName2Idx.size());
    ASSERT_TRUE(snapshot->mAttributeName2Idx.find("a22") != snapshot->mAttributeName2Idx.end());
    ASSERT_TRUE(snapshot->mAttributeName2Idx.find("a33") != snapshot->mAttributeName2Idx.end());

    auto tabletReader = snapshot->GetTabletReader(schemaA);
    ASSERT_TRUE(tabletReader != nullptr);
    tabletReader = snapshot->GetTabletReader(/*idx*/ 0);
    ASSERT_TRUE(tabletReader == nullptr);
    tabletReader = snapshot->GetTabletReader(/*idx*/ 2);
    ASSERT_TRUE(tabletReader != nullptr);

    auto normalTabletReader =
        std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tablet->GetTabletReader());
    AttributeReaderInfoV2 attributeReaderInfo2;
    AttributeReaderInfoV2 attributeReaderInfo3;
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a22", schemaA, attributeReaderInfo2));
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a33", schemaA, attributeReaderInfo3));
    ASSERT_EQ(attributeReaderInfo2.tabletReader, attributeReaderInfo3.tabletReader);
    ASSERT_EQ(attributeReaderInfo2.indexPartReaderType, attributeReaderInfo3.indexPartReaderType);

    auto attrReader2 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a22");
    auto attrReader3 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a33");
    ASSERT_EQ(attrReader2, attributeReaderInfo2.attrReader);
    ASSERT_EQ(attrReader3, attributeReaderInfo3.attrReader);
}

void IndexApplicationTest::TestParseJoinRelationsWithTablet()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);
    IndexPartitionPtr partC = partition::FakeOnlinePartition::Create(schemaC, readerC);
    IndexPartitionPtr partD = partition::FakeOnlinePartition::Create(schemaD, readerD);
    IndexPartitionPtr partE = partition::FakeOnlinePartition::Create(schemaE, readerE);

    std::vector<IndexPartitionPtr> indexPartitions = {partA, partB, partC, partD, partE};
    /*************
      join relations:
      A join C on a2
      subTableA join D on sub_a2
      B join E on b2
      subTableD join C on sub_d2
    **************/
    JoinRelationMap joinRelations;
    JoinField joinField1("a2", "tableC", false, false);
    joinRelations["tableA"] = {joinField1};

    JoinField joinField2("sub_a2", "tableD", false, false);
    joinRelations["subTableA"] = {joinField2};

    JoinField joinField3("b2", "tableE", false, false);
    joinRelations["tableB"] = {joinField3};

    JoinField joinField4("sub_d2", "tableC", false, false);
    joinRelations["subTableD"] = {joinField4};

    const std::string schemaA("tabletSchemaA");
    auto schema = GetDefaultTabletSchema(schemaA, // Field schema
                                         "a1:string; a2:string; a3:string",
                                         // index schema
                                         "pk:primarykey64:a1",
                                         // attribute schema
                                         "a2;a3");
    auto tablet = GetDefaultTablet(schema);
    ASSERT_TRUE(schema != nullptr);
    ASSERT_TRUE(tablet != nullptr);

    std::map<std::string, std::shared_ptr<indexlibv2::framework::ITablet>> tabletMap;
    tabletMap[schemaA] = tablet;

    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));
    ASSERT_TRUE(app.InitByTablet(tabletMap));
    ASSERT_EQ((size_t)11, app.mIndexName2IdMap.Size());
    ASSERT_EQ((size_t)16, app.mAttrName2IdMap.Size());
    ASSERT_EQ((size_t)9, app.mReaderTypeVec.size());

    ReverseJoinRelationMap reverseJoinMap = *(app.mReaderUpdater->GetJoinRelationMap());
    ASSERT_EQ(size_t(3), reverseJoinMap.size());
    ASSERT_EQ(size_t(2), reverseJoinMap["tableC"].size());
    ASSERT_EQ(size_t(1), reverseJoinMap["tableD"].size());
    ASSERT_EQ(size_t(1), reverseJoinMap["tableE"].size());

    EXPECT_EQ(joinField1, reverseJoinMap["tableC"]["tableA"]);
    EXPECT_EQ(joinField4, reverseJoinMap["tableC"]["subTableD"]);
    EXPECT_EQ(joinField2, reverseJoinMap["tableD"]["subTableA"]);
    EXPECT_EQ(joinField3, reverseJoinMap["tableE"]["tableB"]);

    ASSERT_TRUE(app._hasTablet);
    ASSERT_EQ(1, app._tablets.size());
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a2"));
    ASSERT_TRUE(app.mAttrName2IdMap.Exist(schemaA, "a3"));
    ASSERT_TRUE(app.mIndexName2IdMap.Exist(schemaA, "pk"));

    auto snapshot = app.CreateSnapshot();
    auto normalTabletReader =
        std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(tablet->GetTabletReader());
    AttributeReaderInfoV2 attributeReaderInfo2;
    AttributeReaderInfoV2 attributeReaderInfo3;
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a2", schemaA, attributeReaderInfo2));
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV2("a3", schemaA, attributeReaderInfo3));
    ASSERT_EQ(attributeReaderInfo2.tabletReader, attributeReaderInfo3.tabletReader);
    ASSERT_EQ(attributeReaderInfo2.indexPartReaderType, attributeReaderInfo3.indexPartReaderType);

    auto attrReader2 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a2");
    auto attrReader3 = normalTabletReader->GetIndexReader<indexlibv2::index::AttributeReader>("attribute", "a3");
    ASSERT_EQ(attrReader2, attributeReaderInfo2.attrReader);
    ASSERT_EQ(attrReader3, attributeReaderInfo3.attrReader);
}

}} // namespace indexlib::partition
