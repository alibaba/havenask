#include "indexlib/partition/test/reader_snapshot_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"
#include "indexlib/partition/test/fake_online_partition.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReaderSnapshotTest);

namespace {
class FakeIndexPartitionReader : public FakeIndexPartitionReaderBase
{
public:
    FakeIndexPartitionReader() { mAttrReader.reset(new index::SingleValueAttributeReader<uint32_t>(NULL)); }

public:
    virtual const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const { return mAttrReader; }

private:
    index::AttributeReaderPtr mAttrReader;
};
}; // namespace

ReaderSnapshotTest::ReaderSnapshotTest() {}

ReaderSnapshotTest::~ReaderSnapshotTest() {}

void ReaderSnapshotTest::CaseSetUp()
{
    schemaA.reset(new IndexPartitionSchema());
    PartitionSchemaMaker::MakeSchema(schemaA,
                                     // Field schema
                                     "a1:string; a2:string; a3:string; b1:string",
                                     // index schema
                                     "a1:string:a1",
                                     // attribute schema
                                     "a2;a3;b1;a1",
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
                                     "b2;b3;b1",
                                     // summary schema
                                     "");
    schemaB->SetSchemaName("tableB");
    FakeIndexPartitionReaderBase* reader = new FakeIndexPartitionReader();
    readerA.reset(reader);
    reader->SetSchema(schemaA);
    reader = new FakeIndexPartitionReader();
    readerB.reset(reader);
    reader->SetSchema(schemaB);
}

void ReaderSnapshotTest::AddSub()
{
    IndexPartitionSchemaPtr subSchemaA(new IndexPartitionSchema());
    FakeIndexPartitionReaderBasePtr subReaderA(new FakeIndexPartitionReader());
    PartitionSchemaMaker::MakeSchema(subSchemaA,
                                     // Field schema
                                     "sub_a1:string; sub_a2:string; sub_a3:string",
                                     // index schema
                                     "sub_a1:string:sub_a1",
                                     // attribute schema
                                     "sub_a2;sub_a3",
                                     // summary schema
                                     "");
    subSchemaA->SetSchemaName("sub_schema");
    schemaA->SetSubIndexPartitionSchema(subSchemaA);
    subReaderA->SetSchema(subSchemaA);
    FakeIndexPartitionReaderBase* reader = dynamic_cast<FakeIndexPartitionReaderBase*>(readerA.get());
    assert(reader);
    reader->SetSubIndexPartitionReader(subReaderA);
}

void ReaderSnapshotTest::CaseTearDown() {}

void ReaderSnapshotTest::TestSimpleProcess()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);

    JoinRelationMap joinRelations;
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinRelations));

    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    IndexPartitionReaderPtr indexPartReader = snapshot->GetIndexPartitionReader("tableA");
    ASSERT_TRUE(indexPartReader);
    ASSERT_EQ(string("tableA"), indexPartReader->GetSchema()->GetSchemaName());

    // test get attribute reader
    AttributeReaderInfo info;
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV1("a1", "", info));
    ASSERT_EQ(string("tableA"), info.indexPartReader->GetSchema()->GetSchemaName());
    ASSERT_FALSE(snapshot->GetAttributeReaderInfoV1("b1", "", info));
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV1("b1", "tableA", info));
    ASSERT_EQ(string("tableA"), info.indexPartReader->GetSchema()->GetSchemaName());
    ASSERT_TRUE(snapshot->GetAttributeReaderInfoV1("sub_a2", "", info));
    ASSERT_EQ(string("sub_schema"), info.indexPartReader->GetSchema()->GetSchemaName());

    // test GetTableNameByAttribute
    ASSERT_EQ(string("tableA"), snapshot->GetTableNameByAttribute("a1", "tableA"));
    ASSERT_EQ(string("tableA"), snapshot->GetTableNameByAttribute("a1", "tableB"));
    ASSERT_EQ(string("tableA"), snapshot->GetTableNameByAttribute("a1", ""));
}

void ReaderSnapshotTest::TestLeadingTable()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);

    JoinRelationMap joinMap;
    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinMap));

    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    IndexPartitionReaderPtr indexPartReader = snapshot->GetIndexPartitionReader("tableA");
    ASSERT_TRUE(indexPartReader);
    ASSERT_EQ(string("tableA"), indexPartReader->GetSchema()->GetSchemaName());

    // test get attribute reader
    AttributeReaderInfo info;
    PartitionReaderSnapshotPtr leadingSnapshot(snapshot->CreateLeadingSnapshotReader("tableA"));
    ASSERT_TRUE(leadingSnapshot);
    ASSERT_TRUE(leadingSnapshot->GetAttributeReaderInfoV1("a1", "", info));
    ASSERT_EQ(string("tableA"), info.indexPartReader->GetSchema()->GetSchemaName());
    ASSERT_TRUE(leadingSnapshot->GetAttributeReaderInfoV1("b1", "", info));
    ASSERT_EQ(string("tableA"), info.indexPartReader->GetSchema()->GetSchemaName());
}

void ReaderSnapshotTest::TestLeadingTableWithJoinTable()
{
    AddSub();
    IndexPartitionPtr partA = partition::FakeOnlinePartition::Create(schemaA, readerA);
    IndexPartitionPtr partB = partition::FakeOnlinePartition::Create(schemaB, readerB);

    vector<IndexPartitionPtr> indexPartitions;
    indexPartitions.push_back(partA);
    indexPartitions.push_back(partB);

    JoinField joinField("b1", "tableB", false, true);
    vector<JoinField> joinFields;
    joinFields.push_back(joinField);
    JoinRelationMap joinMap;
    joinMap["tableA"] = joinFields;

    IndexApplication app;
    ASSERT_TRUE(app.Init(indexPartitions, joinMap));
    PartitionReaderSnapshotPtr snapshot = app.CreateSnapshot();
    ASSERT_TRUE(snapshot);
    IndexPartitionReaderPtr indexPartReader = snapshot->GetIndexPartitionReader("tableA");
    ASSERT_TRUE(indexPartReader);
    ASSERT_EQ(string("tableA"), indexPartReader->GetSchema()->GetSchemaName());

    AttributeReaderInfo info;
    // test get attribute reader from leading snapshot reader
    PartitionReaderSnapshotPtr leadingSnapshot(snapshot->CreateLeadingSnapshotReader("tableA"));
    ASSERT_TRUE(leadingSnapshot);
    ASSERT_TRUE(leadingSnapshot->GetAttributeReaderInfoV1("a1", "", info));
    ASSERT_EQ(string("tableA"), info.indexPartReader->GetSchema()->GetSchemaName());
    ASSERT_TRUE(leadingSnapshot->GetAttributeReaderInfoV1("b2", "", info));
    ASSERT_EQ(string("tableB"), info.indexPartReader->GetSchema()->GetSchemaName());
}
}} // namespace indexlib::partition
