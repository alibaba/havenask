#include "indexlib/partition/test/table_reader_container_updater_unittest.h"
#include "indexlib/partition/test/fake_index_partition_creator.h"
#include "indexlib/partition/test/fake_online_partition.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/partition/test/mock_index_partition.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(table);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, TableReaderContainerUpdaterTest);

TableReaderContainerUpdaterTest::TableReaderContainerUpdaterTest()
{
}

TableReaderContainerUpdaterTest::~TableReaderContainerUpdaterTest()
{
}

void TableReaderContainerUpdaterTest::CaseSetUp()
{
    PepareSchema(5);
    AddSub({0, 1, 3});    
}

void TableReaderContainerUpdaterTest::CaseTearDown()
{
}

IndexPartitionPtr TableReaderContainerUpdaterTest::GetIndexPartition(std::string name)
{
    assert(mTableName2PartitionIdMap.find(name) != mTableName2PartitionIdMap.end());
    return mIndexPartitions[mTableName2PartitionIdMap[name]];
}

IndexPartitionReaderPtr TableReaderContainerUpdaterTest::GetIndexPartitionReader(std::string name)
{
    assert(mTableName2PartitionIdMap.find(name) != mTableName2PartitionIdMap.end());
    return mReaders[mTableName2PartitionIdMap[name]];
}

void TableReaderContainerUpdaterTest::ErasePartition(
        std::vector<IndexPartitionPtr>& indexPartitions,
        string name)
{
    size_t pos = indexPartitions.size();
    for(size_t i = 0; i < indexPartitions.size(); i ++)
    {
        if (indexPartitions[i]->GetSchema()->GetSchemaName() == name)
        {
            pos = i;
        }
    }
    if (pos < indexPartitions.size())
    {
        indexPartitions.erase(indexPartitions.begin() + pos);
    }
        
}

void TableReaderContainerUpdaterTest::TestSimpleProcess()
{
    size_t partitionCount = 5;

    /*************
      join relations:
      A join C on a2
      subTableA join D on sub_a2
      B join E on b2
      subTableD join C on sub_d2 
    **************/

    std::vector<FakeOnlinePartition*> indexPartitions;
    for (auto part: mIndexPartitions)
    {
        indexPartitions.push_back((FakeOnlinePartition*)part.get());
    }
    JoinRelationMap joinRelations;
    vector<JoinField> joinFields;
    joinFields.emplace_back("a2", "tableC", true, false);
    joinFields.emplace_back("sub_a2", "tableD", true, false);
    JoinField joinField1("b2", "tableE", true, false);
    JoinField joinField2("sub_d2", "tableC", true, false);
    joinRelations["tableA"] = joinFields;
    joinRelations["tableB"] = {joinField1};
    joinRelations["tableD"] = {joinField2};

    {
        TableReaderContainer container;
        container.Init(mTableName2PartitionIdMap);

        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));

        ReverseJoinRelationMap& reverseJoinMap = updater.mReverseJoinRelationMap;
        ASSERT_EQ(size_t(3), reverseJoinMap.size());
        ASSERT_EQ(size_t(2), reverseJoinMap["tableC"].size());
        ASSERT_EQ(size_t(1), reverseJoinMap["tableD"].size());
        ASSERT_EQ(size_t(1), reverseJoinMap["tableE"].size());

        EXPECT_EQ(joinField2, reverseJoinMap["tableC"]["tableD"]);
        EXPECT_EQ(joinField1, reverseJoinMap["tableE"]["tableB"]);
        ASSERT_TRUE(container.GetReader("tableA"));
        ASSERT_TRUE(container.GetReader("tableC"));
        ASSERT_TRUE(updater.GetIndexPartitionIdx("tableC")
                    < updater.GetIndexPartitionIdx("tableE"));
        ASSERT_TRUE(updater.GetIndexPartitionIdx("tableC")
                    < updater.GetIndexPartitionIdx("tableA"));
        ASSERT_TRUE(updater.GetIndexPartitionIdx("tableA")
                    < updater.GetIndexPartitionIdx("tableB"));
        ASSERT_TRUE(updater.GetIndexPartitionIdx("tableB")
                    < updater.GetIndexPartitionIdx("tableD"));
    }

    {
        // erase tableE, test tablename not exist, 
        TableReaderContainer container;
        container.Init(mTableName2PartitionIdMap);

        std::vector<IndexPartitionPtr> indexPartitions2 = mIndexPartitions;
        ErasePartition(indexPartitions2, "tableE");
        TableReaderContainerUpdater updater;
        ASSERT_FALSE(updater.Init(indexPartitions2, joinRelations, &container));
    }
    {
        // join field not exist
        TableReaderContainer container;
        container.Init(mTableName2PartitionIdMap);

        JoinRelationMap joinRelations2 = joinRelations;
        joinRelations2["tableA"] = {JoinField("sub_a5", "tableD", false, false)};
        TableReaderContainerUpdater updater;
        ASSERT_FALSE(updater.Init(mIndexPartitions, joinRelations2, &container));
    }
    {
        // reserve memory fail
        TableReaderContainer container;
        container.Init(mTableName2PartitionIdMap);

        auto mockPart = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableA"));
        mockPart->SetLeftQuota(0);
        TableReaderContainerUpdater updater;
        ASSERT_FALSE(updater.Init(mIndexPartitions, joinRelations, &container));
        ASSERT_FALSE(container.GetReader("tableA"));
        ASSERT_FALSE(container.GetReader("tableC"));
        mockPart->SetLeftQuota(1);
    }

    {
        // catch exception when add virtual attribute
        TableReaderContainer container;
        container.Init(mTableName2PartitionIdMap);

        auto mockPart = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableA"));
        mockPart->SetThrowException(true);
        TableReaderContainerUpdater updater;
        ASSERT_FALSE(updater.Init(mIndexPartitions, joinRelations, &container));
        ASSERT_FALSE(container.GetReader("tableA"));
        ASSERT_FALSE(container.GetReader("tableC"));
        for (size_t i = 0; i < partitionCount; i ++)
        {
            ASSERT_FALSE(indexPartitions[i]->GetReaderUpdater()->HasUpdater(&updater));
        }
        mockPart->SetThrowException(false);
    }

}

void TableReaderContainerUpdaterTest::TestSwitchUpdater()
{

    TableReaderContainer container;
    container.Init(mTableName2PartitionIdMap);

    std::vector<FakeOnlinePartition*> indexPartitions;
    for (auto part: mIndexPartitions)
    {
        indexPartitions.push_back((FakeOnlinePartition*)part.get());
    }
    JoinRelationMap joinRelations;
    vector<JoinField> joinFields;
    joinFields.emplace_back("a2", "tableC", true, false);
    joinFields.emplace_back("sub_a2", "tableD", true, false);
    JoinField joinField1("b2", "tableE", true, false);
    JoinField joinField2("sub_d2", "tableC", true, false);
    joinRelations["tableA"] = joinFields;
    joinRelations["tableB"] = {joinField1};
    //joinRelations["tableD"] = {joinField2};
    
    TableReaderContainerUpdaterPtr updater, updater2;
    updater.reset(new TableReaderContainerUpdater);
    ASSERT_TRUE(updater->Init(mIndexPartitions, joinRelations, &container));

    auto erasePart = GetIndexPartition("tableE");
    ErasePartition(mIndexPartitions, "tableE");
    mTableName2PartitionIdMap.clear();
    for (size_t i = 0; i < mIndexPartitions.size(); i ++)
    {
        mTableName2PartitionIdMap[mIndexPartitions[i]->GetSchema()->GetSchemaName()] = i;
    }

    TableReaderContainer container2;
    JoinRelationMap joinRelations2 = joinRelations;
    container2.Init(mTableName2PartitionIdMap);

    updater2.reset(new TableReaderContainerUpdater);
    joinRelations2.erase("tableB");

    ASSERT_TRUE(updater2->Init(mIndexPartitions, joinRelations2, &container2));
    updater.reset();
    for (auto& part: mIndexPartitions)
    {
        FakeOnlinePartition* fakePart = (FakeOnlinePartition*)(part.get());
        ASSERT_TRUE(fakePart->GetReaderUpdater()->HasUpdater(updater2.get()));
    }
    FakeOnlinePartition* fakePart = (FakeOnlinePartition*)(erasePart.get());
    ASSERT_TRUE(fakePart->GetReaderUpdater()->Size() == 0);
}

void TableReaderContainerUpdaterTest::TestUpdatePartition()
{
    size_t partitionCount = 6;
    PepareSchema(partitionCount);
    AddSub({0, 1, 3});
    TableReaderContainer container;
    container.Init(mTableName2PartitionIdMap);
    /*************
      join relations:
      A join C on a2
      A join F on a3
      subTableA join D on sub_a2
      B join E on b2
      subTableD join C on sub_d2 
    **************/
    
    JoinRelationMap joinRelations;
    vector<JoinField> joinFields;
    joinFields.emplace_back("a2", "tableC", true, false);
    joinFields.emplace_back("sub_a2", "tableD", true, false);
    joinFields.emplace_back("a3", "tableF", false, false);
    joinRelations["tableA"] = joinFields;
    joinRelations["tableB"] = {JoinField("b2", "tableE", true, false)};
    joinRelations["tableD"] = {JoinField("sub_d2", "tableC", true, false)};

    {
        PepareSchema(partitionCount);
        AddSub({0, 1, 3});
        // update main table
        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));
        auto readerA = GetIndexPartitionReader("tableA");
        readerA.reset(new FakeIndexPartitionReaderBase(readerA->GetSchema(), 1));
        auto mockPart = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableA"));
        mockPart->SetReader(readerA);
        ASSERT_TRUE(updater.Update(readerA));
        ASSERT_EQ(readerA, container.GetReader("tableA"));
    }

    {
        PepareSchema(partitionCount);
        AddSub({0, 1, 3});
        // update aux table
        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));
        for (size_t i = 0; i < mIndexPartitions.size(); i ++)
        {
            mReaders[i] = mIndexPartitions[i]->GetReader();
        }
        ASSERT_EQ(GetIndexPartition("tableB")->GetReader().get(), container.GetReader("tableB").get());
        auto readerC = GetIndexPartitionReader("tableC");
        readerC.reset(new FakeIndexPartitionReaderBase(readerC->GetSchema(), 1));
        auto mockPartC = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableC"));
        mockPartC->SetReader(readerC);
        ASSERT_TRUE(updater.Update(readerC));
        ASSERT_NE(GetIndexPartitionReader("tableA"), container.GetReader("tableA"));
        ASSERT_EQ(GetIndexPartitionReader("tableB"), container.GetReader("tableB"));
        ASSERT_EQ(readerC, container.GetReader("tableC"));
        ASSERT_NE(GetIndexPartitionReader("tableD"), container.GetReader("tableD"));
        ASSERT_EQ(GetIndexPartitionReader("tableE"), container.GetReader("tableE"));        
    }

    {
        PepareSchema(partitionCount);
        AddSub({0, 1, 3});
        // reserve memory false
        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));
        for (size_t i = 0; i < mIndexPartitions.size(); i ++)
            mReaders[i] = mIndexPartitions[i]->GetReader();
        auto readerC = GetIndexPartitionReader("tableC");
        readerC.reset(new FakeIndexPartitionReaderBase(readerC->GetSchema(), 2));
        auto mockPartC = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableC"));
        auto mockPartA = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableA"));
        auto mockPartD = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableD"));
        mockPartC->SetReader(readerC);
        mockPartA->SetLeftQuota(0);
        ASSERT_FALSE(updater.Update(readerC));
        ASSERT_NE(readerC, container.GetReader("tableC"));
        ASSERT_EQ(GetIndexPartitionReader("tableD"), container.GetReader("tableD"));
        
        mockPartA->SetLeftQuota(1);
        ASSERT_TRUE(updater.Update(readerC));
        ASSERT_EQ(readerC, container.GetReader("tableC"));
        ASSERT_NE(GetIndexPartitionReader("tableD"), container.GetReader("tableD"));

        readerC.reset(new FakeIndexPartitionReaderBase(readerC->GetSchema(), 3));
        mockPartD->SetLeftQuota(0);
        ASSERT_FALSE(updater.Update(readerC));
    }

    {
        // throw io exception add virtual attribute
        PepareSchema(partitionCount);
        AddSub({0, 1, 3});
        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));
        for (size_t i = 0; i < mIndexPartitions.size(); i ++)
            mReaders[i] = mIndexPartitions[i]->GetReader();
        auto readerC = GetIndexPartitionReader("tableC");
        auto readerD = GetIndexPartitionReader("tableD");
        readerC.reset(new FakeIndexPartitionReaderBase(readerC->GetSchema(), 2));
        auto mockPartC = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableC"));
        auto mockPartA = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableA"));
        mockPartC->SetReader(readerC);
        mockPartA->SetThrowException(true);
        ASSERT_FALSE(updater.Update(readerC));
        ASSERT_NE(readerC, container.GetReader("tableC"));
        ASSERT_EQ(readerD, container.GetReader("tableD"));

        mockPartA->SetThrowException(false);
        ASSERT_TRUE(updater.Update(readerC));
        ASSERT_EQ(readerC, container.GetReader("tableC"));
        ASSERT_NE(readerD, container.GetReader("tableD"));
    }

    {
        PepareSchema(partitionCount);
        AddSub({0, 1, 3});
        TableReaderContainerUpdater updater;
        ASSERT_TRUE(updater.Init(mIndexPartitions, joinRelations, &container));
        for (size_t i = 0; i < mIndexPartitions.size(); i ++)
            mReaders[i] = mIndexPartitions[i]->GetReader();
        auto readerF = GetIndexPartitionReader("tableF");
        auto readerA = GetIndexPartitionReader("tableA");
        IndexPartitionReaderPtr newreader(
                new FakeIndexPartitionReaderBase(readerF->GetSchema(), 3));
        auto mockPart = DYNAMIC_POINTER_CAST(FakeOnlinePartition, GetIndexPartition("tableF"));
        mockPart->SetReader(newreader);
        ASSERT_TRUE(updater.Update(newreader));
        ASSERT_NE(readerF, container.GetReader("tableF"));
        ASSERT_EQ(newreader, container.GetReader("tableF"));
        ASSERT_EQ(readerA, container.GetReader("tableA"));        
    }
}

void TableReaderContainerUpdaterTest::PepareSchema(size_t schemaCount)
{
    mSchemas.resize(schemaCount);
    mReaders.resize(schemaCount);
    mIndexPartitions.clear();    
    char startA = 'a';
    for (size_t i = 0; i < mSchemas.size(); i ++)
    {
        mSchemas[i].reset(new IndexPartitionSchema());
        mReaders[i].reset(new FakeIndexPartitionReaderBase(mSchemas[i]));
        auto schema = mSchemas[i];
        string field1, field2, field3;
        field1 += startA; field1 += "1";
        field2 += startA; field2 += "2";
        field3 += startA; field3 += "3";
        startA ++;
        PartitionSchemaMaker::MakeSchema(schema,
                //Field schema
                field1 + ":string; " + field2 + ":string; " + field3 + ":string",
                //index schema
                field1 + ":string:" + field1,
                //attribute schema
                field2 +";" + field3,
                //summary schema
                "");
        schema->SetSchemaName(std::string("table") + (char)('A' + i));
        mIndexPartitions.emplace_back(new FakeOnlinePartition(
                        mSchemas[i], mReaders[i]));
        mIndexPartitions[i]->Open("", "", mSchemas[i], mOptions);
        auto mockPart = DYNAMIC_POINTER_CAST(FakeOnlinePartition, mIndexPartitions[i]);
        mockPart->SetLeftQuota(1);
    }
    std::sort(mIndexPartitions.begin(), mIndexPartitions.end(),
              [=](const IndexPartitionPtr& a, const IndexPartitionPtr& b)->bool
              {
                  return a->GetSchema()->GetSchemaName() > b->GetSchema()->GetSchemaName();
              });
    for (size_t i = 0; i < mIndexPartitions.size(); i ++)
    {
        auto part = mIndexPartitions[i];
        mTableName2PartitionIdMap[part->GetSchema()->GetSchemaName()] = i;
        mReaders[i] = part->GetReader();
    }
}

void TableReaderContainerUpdaterTest::AddSub(std::vector<size_t> subIdx)
{

    for (auto i: subIdx)
    {
        auto schema = mSchemas[i];
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema());
        FakeIndexPartitionReaderBasePtr subReader(new FakeIndexPartitionReaderBase(subSchema));
        string field1 = "sub_", field2 = "sub_", field3 = "sub_";
        char startA = 'a' + i;
        field1 += startA; field1 += "1";
        field2 += startA; field2 += "2";
        field3 += startA; field3 += "3";
        PartitionSchemaMaker::MakeSchema(subSchema,
                //Field schema
                field1 + ":string; " + field2 + ":string; " + field3 + ":string",
                //index schema
                field1 + ":string:" + field1,
                //attribute schema
                field2 +";" + field3,
                //summary schema
                "");
        char tableChar = 'A' + i;
        subSchema->SetSchemaName(std::string("subTable") + tableChar);
        schema->SetSubIndexPartitionSchema(subSchema);
        auto reader = GetIndexPartitionReader(string("table") + tableChar);
        auto readerBase = dynamic_cast<FakeIndexPartitionReaderBase*>(reader.get());
        assert(readerBase);
        readerBase->SetSubIndexPartitionReader(subReader);
    }
}

void TableReaderContainerUpdaterTest::TestInitPartitions()
{
    std::vector<IndexPartitionPtr> indexPartitions;

    partition::IndexPartitionResource resource;
    resource.taskScheduler.reset(new TaskScheduler());
    resource.memoryQuotaController.reset(new MemoryQuotaController(10000));
    IndexPartitionSchemaPtr customSchema(new IndexPartitionSchema("table1"));
    customSchema->SetTableType("customized");
    IndexPartitionPtr part1(new CustomOnlinePartition("table1", resource));
    part1->mSchema = customSchema;
    indexPartitions.push_back(part1);

    IndexPartitionSchemaPtr schema2(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schema2, "b1:string; b2:string; b3:string",
                                     "b1:string:b1", "b2;b3", "");
    schema2->SetSchemaName("table2");
    IndexPartitionPtr part2(new OnlinePartition("table2", resource));
    part2->mSchema = schema2;
    indexPartitions.push_back(part2);

    JoinRelationMap joinRelations;
    JoinField joinField1("b2", "table2", true, false);
    joinRelations["table1"] = {joinField1};
    
    TableReaderContainerUpdaterPtr updater;
    updater.reset(new TableReaderContainerUpdater);
    // custom partition not support join
    ASSERT_FALSE(updater->InitPartitions(indexPartitions, joinRelations));

    JoinRelationMap joinRelations2;
    ASSERT_TRUE(updater->InitPartitions(indexPartitions, joinRelations2));
}

IE_NAMESPACE_END(partition);

