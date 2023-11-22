#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/test/doc_id_manager_test_util.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/build_test_util.h"

namespace indexlib { namespace partition {
namespace {
using test::SingleFieldPartitionDataProvider;
}
class InplaceModifierTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    InplaceModifierTest() {}
    ~InplaceModifierTest() {}

    DECLARE_CLASS_NAME(InplaceModifierTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

protected:
    static void CheckAttribute(const index::AttributeReader& reader, docid_t docid, int32_t expectValue);
    static void CheckAttributeNull(const index::AttributeReader& reader, docid_t docid, bool expectNull);
    static void CheckDeletionMap(const index::DeletionMapReaderPtr& reader, const std::string& docIdStrs,
                                 bool isDeleted);
    static void CheckIndex(index::InvertedIndexReader* reader, std::vector<std::string> terms,
                           std::vector<std::vector<docid_t>> expectedDocIds);
    static SingleFieldPartitionDataProvider CreatePartitionDataProvider(int buildMode);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, InplaceModifierTest);

void InplaceModifierTest::CheckAttribute(const index::AttributeReader& reader, docid_t docid, int32_t expectValue)
{
    std::string value;
    reader.Read(docid, value);
    EXPECT_EQ(std::to_string(expectValue), value);
}

void InplaceModifierTest::CheckAttributeNull(const index::AttributeReader& reader, docid_t docid, bool expectNull)
{
    std::string value;
    reader.Read(docid, value);
    if (expectNull) {
        EXPECT_EQ("__NULL__", value);
    } else {
        EXPECT_NE("__NULL__", value);
    }
}

void InplaceModifierTest::CheckDeletionMap(const index::DeletionMapReaderPtr& reader, const std::string& docIdStrs,
                                           bool isDeleted)
{
    std::vector<docid_t> docIds;
    autil::StringUtil::fromString(docIdStrs, docIds, ",");
    for (size_t i = 0; i < docIds.size(); i++) {
        EXPECT_EQ(isDeleted, reader->IsDeleted(docIds[i]));
    }
}

SingleFieldPartitionDataProvider InplaceModifierTest::CreatePartitionDataProvider(int buildMode)
{
    config::IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(buildMode, &options);
    return SingleFieldPartitionDataProvider(options);
}

TEST_P(InplaceModifierTest, TestSimpleProcess)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "int32", test::SFP_ATTRIBUTE);
    provider.Build("0,1,2,3,4#5,6,7", test::SFP_OFFLINE);
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionOptions options;
    IndexPartitionReaderPtr reader(new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    reader->Open(partitionData);
    InplaceModifierPtr modifier(new InplaceModifier(schema));
    modifier->Init(reader, inMemSegment);

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM, false);

    document::NormalDocumentPtr document(new document::NormalDocument);
    document->SetDocOperateType(DELETE_DOC);
    document::IndexDocumentPtr indexDocument(new document::IndexDocument(document->GetPool()));
    document->SetIndexDocument(indexDocument);

    indexDocument->SetPrimaryKey("0");
    manager.Process(document);
    modifier->RemoveDocument(document);

    indexDocument->SetPrimaryKey("10");
    manager.Process(document);
    modifier->RemoveDocument(document);

    index::DeletionMapReaderPtr deletionmapReader = partitionData->GetDeletionMapReader();
    EXPECT_TRUE(deletionmapReader->IsDeleted(0));
    EXPECT_TRUE(deletionmapReader->IsDeleted(10));
    EXPECT_FALSE(deletionmapReader->IsDeleted(1));
}

TEST_P(InplaceModifierTest, TestDeleteDocument)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "int32", test::SFP_PK_INDEX);
    provider.Build("0,1,2,3", test::SFP_OFFLINE);

    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifierPtr modifier(new InplaceModifier(schema));

    config::IndexPartitionOptions options;
    IndexPartitionReaderPtr onlineReader(
        new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    onlineReader->Open(partitionData);
    modifier->Init(onlineReader, inMemSegment);

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM, false);

    index::DeletionMapReaderPtr reader = partitionData->GetDeletionMapReader();
    CheckDeletionMap(reader, "0,1,2,3", false);

    // check delete built and building doc
    std::vector<document::NormalDocumentPtr> docs = provider.CreateDocuments("delete:0,add:4,delete:4");

    index_base::SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    EXPECT_TRUE(manager.Process(docs[0]));
    EXPECT_TRUE(modifier->RemoveDocument(docs[0]));
    CheckDeletionMap(reader, "0", true);
    CheckDeletionMap(reader, "1,2,3", false);

    EXPECT_TRUE(manager.Process(docs[1]));
    segWriter->AddDocument(docs[1]);
    CheckDeletionMap(reader, "1,2,3,4", false);

    EXPECT_TRUE(manager.Process(docs[2]));
    EXPECT_TRUE(modifier->RemoveDocument(docs[2]));
    CheckDeletionMap(reader, "0,4", true);
    CheckDeletionMap(reader, "1,2,3", false);

    // check delete fail
    docs = provider.CreateDocuments("delete:5,delete:1");
    // check remove non exist doc
    EXPECT_FALSE(manager.Process(docs[0]));

    // check update doc without pk
    docs[1]->SetIndexDocument(nullptr);
    EXPECT_FALSE(manager.Process(docs[1]));
}

TEST_P(InplaceModifierTest, TestUpdateDocument)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "int32", test::SFP_ATTRIBUTE);
    provider.Build("0,1,2,3", test::SFP_OFFLINE);

    config::AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifierPtr modifier(new InplaceModifier(schema));

    config::IndexPartitionOptions options;
    IndexPartitionReaderPtr partReader(
        new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);
    modifier->Init(partReader, inMemSegment);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM, false);

    // check update built and building doc
    std::vector<document::NormalDocumentPtr> docs = provider.CreateDocuments("update_field:0:3,add:4,update_field:4:6");

    index::SingleValueAttributeReader<int32_t> reader;
    reader.Open(attrConfig, partitionData, index::PAS_APPLY_NO_PATCH);

    index_base::SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    EXPECT_TRUE(manager.Process(docs[0]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[0]));
    EXPECT_TRUE(manager.Process(docs[1]));
    segWriter->AddDocument(docs[1]);
    EXPECT_TRUE(manager.Process(docs[2]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[2]));

    CheckAttribute(reader, 0, 3);
    CheckAttribute(reader, 4, 6);

    // check update fail
    docs = provider.CreateDocuments("update_field:5:3,update_field:0:6,update_field:1:7");
    // check update non exist doc
    EXPECT_FALSE(manager.Process(docs[0]));

    // check update doc without pk
    docs[1]->SetIndexDocument(nullptr);
    EXPECT_FALSE(manager.Process(docs[1]));

    // check update doc without attribute doc
    docs[1]->SetAttributeDocument(nullptr);
    EXPECT_FALSE(manager.Process(docs[1]));
}

TEST_P(InplaceModifierTest, TestUpdateDocumentWithoutSchema)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "text", test::SFP_INDEX);
    provider.Build("0", test::SFP_OFFLINE);
    provider.Build("", test::SFP_REALTIME);

    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();

    config::IndexPartitionOptions options;
    IndexPartitionReaderPtr partReader(
        new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);
    InplaceModifierPtr modifier(new InplaceModifier(schema));
    modifier->Init(partReader, inMemSegment);
    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM, false);

    provider.Build("1", test::SFP_REALTIME);

    document::NormalDocumentPtr doc(new document::NormalDocument);
    doc->SetDocOperateType(UPDATE_FIELD);
    document::IndexDocumentPtr indexDoc(new document::IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    document::AttributeDocumentPtr attrDoc(new document::AttributeDocument);
    doc->SetAttributeDocument(attrDoc);

    indexDoc->SetPrimaryKey("0");
    EXPECT_FALSE(manager.Process(doc));
    indexDoc->SetPrimaryKey("1");
    EXPECT_FALSE(manager.Process(doc));
}

TEST_P(InplaceModifierTest, TestUpdateWithNullField)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "int32", test::SFP_ATTRIBUTE, /*enableNull=*/true);
    provider.Build("0,1,2,3,__NULL__", test::SFP_OFFLINE);

    config::AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifierPtr modifier(new InplaceModifier(schema));

    config::IndexPartitionOptions options;
    IndexPartitionReaderPtr partReader(
        new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);
    modifier->Init(partReader, inMemSegment);

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/
                               false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM, false);

    // check update built and building doc
    std::vector<document::NormalDocumentPtr> docs =
        provider.CreateDocuments("update_field:0:__NULL__,add:__NULL__,update_field:4:6");

    index::SingleValueAttributeReader<int32_t> reader;
    reader.Open(attrConfig, partitionData, index::PatchApplyStrategy::PAS_APPLY_NO_PATCH);

    index_base::SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    EXPECT_TRUE(manager.Process(docs[0]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[0]));
    EXPECT_TRUE(manager.Process(docs[1]));
    segWriter->AddDocument(docs[1]);
    EXPECT_TRUE(manager.Process(docs[2]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[2]));

    CheckAttributeNull(reader, 0, /*expectNull=*/true);
    CheckAttributeNull(reader, 1, /*expectNull=*/false);
    CheckAttribute(reader, 1, 1);
    CheckAttributeNull(reader, 4, /*expectNull=*/false);
    CheckAttribute(reader, 4, 6);
    CheckAttributeNull(reader, 5, /*expectNull=*/true);
}

TEST_P(InplaceModifierTest, TestUpdateIndexDocument)
{
    SingleFieldPartitionDataProvider provider = CreatePartitionDataProvider(GetParam());
    provider.Init(GET_TEMP_DATA_PATH(), "int64", test::SFP_INDEX);
    provider.GetSchema()
        ->GetIndexSchema()
        ->GetIndexConfig(SingleFieldPartitionDataProvider::INDEX_NAME_TAG)
        ->TEST_SetIndexUpdatable(true);
    provider.GetSchema()
        ->GetIndexSchema()
        ->GetIndexConfig(SingleFieldPartitionDataProvider::INDEX_NAME_TAG)
        ->SetOptionFlag(OPTION_FLAG_NONE);
    // Add doc id 0(pk=0) with term 0 in default field, doc id 1(pk=1) with term 1 in default field
    provider.Build("0,1", test::SFP_OFFLINE);
    // Add doc id 2(pk=2) with term 2 in default field, doc id 3(pk=3) with term 3 in default field
    provider.Build("2,3", test::SFP_REALTIME);

    config::IndexConfigPtr indexConfig = provider.GetIndexConfig();
    index_base::PartitionDataPtr partitionData = provider.GetPartitionData();
    index_base::InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    config::IndexPartitionSchemaPtr schema = provider.GetSchema();
    InplaceModifierPtr modifier(new InplaceModifier(schema));

    config::IndexPartitionOptions options = provider.GetOptions();
    IndexPartitionReaderPtr partReader(
        new OnlinePartitionReader(options, schema, /*SearchCachePartitionWrapperPtr=*/nullptr));
    partReader->Open(partitionData);
    modifier->Init(partReader, inMemSegment);

    index::InvertedIndexReaderPtr reader =
        partReader->GetInvertedIndexReader(/*indexName=*/SingleFieldPartitionDataProvider::INDEX_NAME_TAG);
    CheckIndex(reader.get(), /*terms=*/ {"0", "1", "2", "3", "4", "6"},
               /*expectedDocIds=*/ {{0}, {1}, {2}, {3}, {}, {}});

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    manager.Reinit(partitionData, modifier, inMemSegment->GetSegmentWriter(),
                   PartitionWriter::BuildMode::BUILD_MODE_STREAM,
                   /*delayDedupDocument=*/false);

    // check update built and building doc
    // Create update doc pk 1, term 3 in default field; add doc id 4(pk=4) with term 4 in default field; update doc
    // pk=4 with term 6 in default field
    std::vector<document::NormalDocumentPtr> docs = provider.CreateDocuments("update_field:1:3,add:4,update_field:4:6");
    ASSERT_EQ(docs.size(), 3);
    ASSERT_TRUE(docs[0]->GetPrimaryKey() == "1");
    ASSERT_TRUE(docs[1]->GetPrimaryKey() == "4");
    ASSERT_TRUE(docs[2]->GetPrimaryKey() == "4");
    docs[0]->GetIndexDocument()->SetModifiedTokens(std::vector<document::ModifiedTokens> {
        document::ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/1,
                                                            /*termKeys=*/ {-1, 3})});
    docs[2]->GetIndexDocument()->SetModifiedTokens(std::vector<document::ModifiedTokens> {
        document::ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/1,
                                                            /*termKeys=*/ {-4, 6})});

    index_base::SegmentWriterPtr segWriter = inMemSegment->GetSegmentWriter();
    EXPECT_TRUE(manager.Process(docs[0]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[0]));
    EXPECT_TRUE(manager.Process(docs[1]));
    segWriter->AddDocument(docs[1]);
    EXPECT_TRUE(manager.Process(docs[2]));
    EXPECT_TRUE(modifier->UpdateDocument(docs[2]));

    CheckIndex(reader.get(), /*terms=*/ {"0", "1", "2", "3", "4", "6"},
               /*expectedDocIds=*/ {{0}, {}, {2}, {1, 3}, {}, {4}});
}

INSTANTIATE_TEST_CASE_P(BuildMode, InplaceModifierTest, ::testing::Values(0, 1, 2));

void InplaceModifierTest::CheckIndex(index::InvertedIndexReader* reader, std::vector<std::string> terms,
                                     std::vector<std::vector<docid_t>> expectedDocIds)
{
    IE_LOG(ERROR, "CheckIndex terms: %s, expectedDocIds: %s", autil::StringUtil::toString(terms).c_str(),
           autil::StringUtil::toString(expectedDocIds).c_str());
    assert(terms.size() == expectedDocIds.size());
    for (int i = 0; i < terms.size(); ++i) {
        index::Term term(terms[i], reader->GetIndexName());
        index::PostingIterator* iter = reader->Lookup(term).ValueOrThrow();
        std::unique_ptr<index::PostingIterator> guard(iter);
        if (expectedDocIds[i].empty()) {
            continue;
        }
        ASSERT_NE(iter, nullptr);
        for (int j = 0; j < expectedDocIds[i].size(); ++j) {
            docid_t seekedDocId = (j == 0) ? INVALID_DOCID : expectedDocIds[i][j - 1];
            EXPECT_EQ(expectedDocIds[i][j], iter->SeekDoc(seekedDocId))
                << "Processing term: " << terms[i] << " seekedDocId: " << seekedDocId;
        }
    }
}

}} // namespace indexlib::partition
