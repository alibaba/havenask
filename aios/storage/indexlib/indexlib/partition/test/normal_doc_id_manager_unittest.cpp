#include <string>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/index/normal/deletionmap/in_mem_deletion_map_reader.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/primarykey/composite_primary_key_reader.h"
#include "indexlib/index/primary_key/mock/MockPrimaryKeyIndexReader.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/normal_doc_id_manager.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/test/doc_id_manager_test_util.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class NormalDocIdManagerTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(NormalDocIdManagerTest);
    void CaseSetUp() override
    {
        std::string field = "price:int32;pk:int64;";
        std::string index = "pk:primarykey64:pk;";
        std::string attr = "price;pk";
        std::string summary = "";
        _schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
        _partitionInfo = index::PartitionInfoCreator::CreatePartitionInfo(/*incDocCounts=*/"2,3", /*rtDocCounts=*/"3,2",
                                                                          GET_TEMP_DATA_PATH());
    }

    void CaseTearDown() override {}

private:
    std::vector<document::DocumentPtr> CreateDocuments(std::vector<std::pair<DocOperateType, std::string>> input,
                                                       int64_t startingPrice);
    config::IndexPartitionSchemaPtr CreateSchemaWithVirtualAttribute(bool enableSource = false);
    void CheckInMemorySegmentReader(const config::IndexPartitionSchemaPtr& schema,
                                    const index_base::InMemorySegmentReaderPtr inMemSegReader);

    void TestBatchDocs(NormalDocIdManager* docIdManager,
                       const std::vector<std::pair<DocOperateType, std::string>>& input,
                       const std::vector<docid_t>& expectedDocIds, const std::vector<docid_t>& expectedDeletedDocIds);
    void TestSequence(const std::string& sequence, const std::vector<docid_t>& expectDocIds,
                      const std::set<docid_t>& expectDeletedDocIds);

private:
    config::IndexPartitionSchemaPtr _schema;
    index::PartitionInfoPtr _partitionInfo;
    const docid_t _buildingSegmentBaseDocId = 123;
};

std::vector<document::DocumentPtr>
NormalDocIdManagerTest::CreateDocuments(std::vector<std::pair<DocOperateType, std::string>> input,
                                        int64_t startingPrice)
{
    std::vector<document::DocumentPtr> docs;
    for (const auto& pair : input) {
        std::string docStr = "cmd=" + test::DocumentCreator::GetDocumentTypeStr(pair.first) + ",pk=" + pair.second +
                             ",price=" + autil::StringUtil::toString(startingPrice);
        startingPrice++;
        document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, docStr);
        docs.push_back(doc);
    }
    return docs;
}

config::IndexPartitionSchemaPtr NormalDocIdManagerTest::CreateSchemaWithVirtualAttribute(bool enableSource)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string attribute = "biz30day;price";
    std::string summary = "string1";
    std::string source;
    if (enableSource) {
        source = "string1#price#biz30day";
    }
    config::IndexPartitionSchemaPtr schema =
        test::SchemaMaker::MakeSchema(field, index, attribute, summary, "", source);

    // set default value for attribute
    config::FieldConfigPtr fieldConfig = schema->GetFieldConfig("biz30day");

    // add virtual attribute
    config::FieldConfigPtr virtualFieldConfig(new config::FieldConfig("vir_attr", ft_int64, false));

    // set default value
    virtualFieldConfig->SetDefaultValue("100");

    config::AttributeConfigPtr attrConfig(new config::AttributeConfig(config::AttributeConfig::ct_virtual));
    attrConfig->Init(virtualFieldConfig);
    schema->AddVirtualAttributeConfig(attrConfig);
    return schema;
}

void NormalDocIdManagerTest::CheckInMemorySegmentReader(const config::IndexPartitionSchemaPtr& schema,
                                                        const index_base::InMemorySegmentReaderPtr inMemSegReader)
{
    ASSERT_TRUE(inMemSegReader->GetInMemDeletionMapReader());

    if (schema->NeedStoreSummary()) {
        ASSERT_TRUE(inMemSegReader->GetSummaryReader());
    }

    const config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    for (size_t i = 0; i < indexSchema->GetIndexCount(); i++) {
        config::IndexConfigPtr indexConfig = indexSchema->GetIndexConfig((indexid_t)i);
        ASSERT_TRUE(
            inMemSegReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(indexConfig->GetIndexName()));
    }

    const config::AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        config::AttributeSchema::Iterator iter = attrSchema->Begin();
        for (; iter != attrSchema->End(); iter++) {
            ASSERT_TRUE(inMemSegReader->GetAttributeSegmentReader((*iter)->GetAttrName()));
        }
    }

    const config::AttributeSchemaPtr& virtualAttrSchema = schema->GetAttributeSchema();
    if (virtualAttrSchema) {
        config::AttributeSchema::Iterator iter = virtualAttrSchema->Begin();
        for (; iter != virtualAttrSchema->End(); iter++) {
            ASSERT_TRUE(inMemSegReader->GetAttributeSegmentReader((*iter)->GetAttrName()));
        }
    }
}

TEST_F(NormalDocIdManagerTest, TestValidateDelete) {}

TEST_F(NormalDocIdManagerTest, TestValidateUpdate) {}

TEST_F(NormalDocIdManagerTest, TestProcessSimpleAdd)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> manager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, false, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);

    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=4,price=10");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
}

TEST_F(NormalDocIdManagerTest, TestProcessDeleteFromIndex)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> manager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, false, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);

    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=delete,pk=2,price=10");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(11, doc->GetDocId());
}

TEST_F(NormalDocIdManagerTest, TestProcessDeleteFromBatch)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});

    std::unique_ptr<NormalDocIdManager> docIdManager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, true, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);
    document::NormalDocumentPtr doc;

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=4,price=10");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(docIdManager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=5,price=12");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(docIdManager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId + 1, doc->GetDocId());

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=delete,pk=4,price=10");
    EXPECT_TRUE(docIdManager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=delete,pk=5,price=10");
    EXPECT_TRUE(docIdManager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId + 1, doc->GetDocId());
}

TEST_F(NormalDocIdManagerTest, TestProcessUpdateFromIndex)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> manager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, false, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);

    document::NormalDocumentPtr doc =
        test::DocumentCreator::CreateNormalDocument(_schema, "cmd=update_field,pk=3,price=10");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(12, doc->GetDocId());
}

TEST_F(NormalDocIdManagerTest, TestProcessUpdateFromBatch)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> manager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, true, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);

    document::NormalDocumentPtr doc;

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=4,price=10");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=5,price=12");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId + 1, doc->GetDocId());

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=update_field,pk=4,price=100");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=update_field,pk=5,price=120");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId + 1, doc->GetDocId());
}

TEST_F(NormalDocIdManagerTest, TestGetDeletedDocIdsFromIndex)
{
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> manager = DocIdManagerTestUtil::CreateNormalDocIdManager(
        _schema, true, pkToDocIdMap, _buildingSegmentBaseDocId, _partitionInfo);

    document::NormalDocumentPtr doc;

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=4,price=10");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=add,pk=5,price=12");
    EXPECT_EQ(INVALID_DOCID, doc->GetDocId());
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId + 1, doc->GetDocId());

    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=delete,pk=4,price=10");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(_buildingSegmentBaseDocId, doc->GetDocId());
    doc = test::DocumentCreator::CreateNormalDocument(_schema, "cmd=delete,pk=1,price=10");
    EXPECT_TRUE(manager->Process(doc));
    EXPECT_EQ(10, doc->GetDocId());

    EXPECT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {10, _buildingSegmentBaseDocId}));
}

void NormalDocIdManagerTest::TestBatchDocs(NormalDocIdManager* docIdManager,
                                           const std::vector<std::pair<DocOperateType, std::string>>& input,
                                           const std::vector<docid_t>& expectedDocIds,
                                           const std::vector<docid_t>& expectedDeletedDocIds)
{
    MockPartitionModifierForDocIdManagerPtr mockModifier =
        DYNAMIC_POINTER_CAST(MockPartitionModifierForDocIdManager, docIdManager->TEST_GetPartitionModifier());

    std::vector<document::DocumentPtr> docs = CreateDocuments(input, /*startingPrice=*/1000);
    ASSERT_EQ(expectedDocIds.size(), docs.size());
    for (size_t i = 0; i < expectedDocIds.size(); ++i) {
        document::DocumentPtr doc = docs[i];
        EXPECT_TRUE(docIdManager->Process(doc));
        EXPECT_EQ(expectedDocIds[i], doc->GetDocId());
    }
    EXPECT_THAT(mockModifier->deletedDocIds, testing::UnorderedElementsAreArray(expectedDeletedDocIds));
}

TEST_F(NormalDocIdManagerTest, TestComplex1)
{
    const docid_t segmentBaseDocId = 123;
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> docIdManager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(_schema, true, pkToDocIdMap, segmentBaseDocId, _partitionInfo);
    TestBatchDocs(docIdManager.get(),
                  {{ADD_DOC, /*pk=*/"4"},
                   {UPDATE_FIELD, /*pk=*/"4"},
                   {ADD_DOC, /*pk=*/"4"},
                   {DELETE_DOC,
                    /*pk=*/"4"}},
                  /*expectedDocIds=*/
                  {123, 123, 124, 124},
                  /*expectedDeletedDocIds=*/ {123, 124});
}

TEST_F(NormalDocIdManagerTest, TestComplex2)
{
    const docid_t segmentBaseDocId = 123;
    std::map<std::string, docid_t> pkToDocIdMap({{"1", 10}, {"2", 11}, {"3", 12}});
    std::unique_ptr<NormalDocIdManager> docIdManager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(_schema, true, pkToDocIdMap, segmentBaseDocId, _partitionInfo);
    TestBatchDocs(docIdManager.get(),
                  {{DELETE_DOC, /*pk=*/"2"},
                   {ADD_DOC, /*pk=*/"4"},
                   {ADD_DOC, /*pk=*/"2"},
                   {UPDATE_FIELD, /*pk=*/"2"},
                   {UPDATE_FIELD, /*pk=*/"4"},
                   {DELETE_DOC, /*pk=*/"2"}},
                  /*expectedDocIds=*/
                  {11, 123, 124, 124, 123, 124},
                  /*expectedDeletedDocIds=*/ {11, 124});
}

void NormalDocIdManagerTest::TestSequence(const std::string& opSequence, const std::vector<docid_t>& expectDocIds,
                                          const std::set<docid_t>& expectDeletedDocIds)
{
    assert(opSequence.size() == expectDocIds.size());

    bool batchMode = true;
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(_schema, batchMode, {}, 0, _partitionInfo);
    for (size_t i = 0; i < opSequence.size(); ++i) {
        std::stringstream ss;
        switch (opSequence[i]) {
        case 'A':
            ss << "cmd=add,pk=1,price=" << i;
            break;
        case 'D':
            ss << "cmd=delete,pk=1";
            break;
        case 'U':
            ss << "cmd=update_field,pk=1,price=" << i;
            break;
        default:
            assert(false);
        }
        std::string docString = ss.str();
        document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, docString);

        ASSERT_EQ(expectDocIds[i] != INVALID_DOCID, manager->Process(doc))
            << "Process [" << opSequence.substr(0, i) << "(" << opSequence[i] << ")" << opSequence.substr(i + 1)
            << "] wrong, expect docId [" << expectDocIds[i] << "] != actual [" << doc->GetDocId() << "]";
        ASSERT_EQ(expectDocIds[i], doc->GetDocId())
            << "Process [" << opSequence.substr(0, i) << "(" << opSequence[i] << ")" << opSequence.substr(i + 1)
            << "] wrong, expect docId [" << expectDocIds[i] << "] != actual [" << doc->GetDocId() << "]";
    }
    DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), expectDeletedDocIds);
}

TEST_F(NormalDocIdManagerTest, TestAAA) { TestSequence("AAA", {0, 1, 2}, {0, 1}); }
TEST_F(NormalDocIdManagerTest, TestAAD) { TestSequence("AAD", {0, 1, 1}, {0, 1}); }
TEST_F(NormalDocIdManagerTest, TestAAU) { TestSequence("AAU", {0, 1, 1}, {0}); }
TEST_F(NormalDocIdManagerTest, TestADA) { TestSequence("ADA", {0, 0, 1}, {0}); }
TEST_F(NormalDocIdManagerTest, TestADU) { TestSequence("ADU", {0, 0, INVALID_DOCID}, {0}); }
TEST_F(NormalDocIdManagerTest, TestADD) { TestSequence("ADD", {0, 0, INVALID_DOCID}, {0}); }
TEST_F(NormalDocIdManagerTest, TestAUA) { TestSequence("AUA", {0, 0, 1}, {0}); }
TEST_F(NormalDocIdManagerTest, TestAUD) { TestSequence("AUD", {0, 0, 0}, {0}); }
TEST_F(NormalDocIdManagerTest, TestAUU) { TestSequence("AUU", {0, 0, 0}, {}); }

TEST_F(NormalDocIdManagerTest, TestDAA) { TestSequence("DAA", {INVALID_DOCID, 0, 1}, {}); }
TEST_F(NormalDocIdManagerTest, TestDAD) { TestSequence("DAD", {INVALID_DOCID, 0, 0}, {0}); }
TEST_F(NormalDocIdManagerTest, TestDAU) { TestSequence("DAU", {INVALID_DOCID, 0, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestDDA) { TestSequence("DDA", {INVALID_DOCID, INVALID_DOCID, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestDDU) { TestSequence("DDU", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestDDD) { TestSequence("DDD", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestDUA) { TestSequence("DUA", {INVALID_DOCID, INVALID_DOCID, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestDUD) { TestSequence("DUD", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestDUU) { TestSequence("DUU", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }

TEST_F(NormalDocIdManagerTest, TestUAA) { TestSequence("UAA", {INVALID_DOCID, 0, 1}, {0}); }
TEST_F(NormalDocIdManagerTest, TestUAD) { TestSequence("UAD", {INVALID_DOCID, 0, 0}, {0}); }
TEST_F(NormalDocIdManagerTest, TestUAU) { TestSequence("UAU", {INVALID_DOCID, 0, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestUDA) { TestSequence("UDA", {INVALID_DOCID, INVALID_DOCID, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestUDU) { TestSequence("UDU", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestUDD) { TestSequence("UDD", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestUUA) { TestSequence("UUA", {INVALID_DOCID, INVALID_DOCID, 0}, {}); }
TEST_F(NormalDocIdManagerTest, TestUUD) { TestSequence("UUD", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }
TEST_F(NormalDocIdManagerTest, TestUUU) { TestSequence("UUU", {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID}, {}); }

TEST_F(NormalDocIdManagerTest, TestIsValidDocumentWithNumPk)
{
    config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
    index::PartitionSchemaMaker::MakeSchema(schema, "num:uint32", "pk:primarykey64:num", "", "");
    config::PrimaryKeyIndexConfigPtr pkConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    pkConfig->SetPrimaryKeyHashType(index::pk_number_hash);

    partition::NormalDocIdManager docIdManager(schema, /*enableAutoAdd2Update=*/false);

    document::NormalDocumentPtr doc(new document::NormalDocument);
    doc->SetIndexDocument(document::IndexDocumentPtr(new document::IndexDocument(doc->GetPool())));
    {
        // test pk not number
        doc->GetIndexDocument()->SetPrimaryKey("hel");
        ASSERT_FALSE(docIdManager.ValidateAddDocument(doc));
    }

    {
        // test pk num not valid
        doc->GetIndexDocument()->SetPrimaryKey("-1");
        ASSERT_FALSE(docIdManager.ValidateAddDocument(doc));
    }

    {
        // test pk num valid
        doc->GetIndexDocument()->SetPrimaryKey("1");
        ASSERT_TRUE(docIdManager.ValidateAddDocument(doc));
    }
}

TEST_F(NormalDocIdManagerTest, TestIsValidDocumentWithNoPkDocWhenSchemaHasPk)
{
    config::IndexPartitionSchemaPtr schema(new config::IndexPartitionSchema);
    index::PartitionSchemaMaker::MakeSchema(schema, "string1:string", "pk:primarykey64:string1", "", "");

    document::NormalDocumentPtr doc(new document::NormalDocument);
    document::IndexDocumentPtr indexDoc(new document::IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);

    NormalDocIdManager docIdManager(schema, /*enableAutoAdd2Update=*/false);
    ASSERT_FALSE(docIdManager.ValidateAddDocument(doc));
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutSummaryDoc)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string attribute = "price;biz30day";
    std::string summary = "string1";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, summary);

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    doc->SetSummaryDocument(document::SerializedSummaryDocumentPtr());

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    ASSERT_FALSE(manager.ValidateAddDocument(doc));
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutPkDoc)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string attribute = "price;biz30day";
    std::string summary = "string1";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, summary);

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    document::IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    indexDoc->SetPrimaryKey("");

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);
    ASSERT_FALSE(manager.ValidateAddDocument(doc));
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentNormal)
{
    config::IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();
    // set default value for attribute
    config::FieldConfigPtr fieldConfig = schema->GetFieldConfig("biz30day");
    fieldConfig->SetDefaultValue("10");

    index_base::SegmentData segmentData;
    SingleSegmentWriter segWriter(schema, config::IndexPartitionOptions());
    segWriter.Init(segmentData, std::shared_ptr<framework::SegmentMetrics>(new framework::SegmentMetrics),
                   util::BuildResourceMetricsPtr(), util::CounterMapPtr(), plugin::PluginManagerPtr());
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(schema, false, {}, 0, GET_TEMP_DATA_PATH());

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    segWriter.AddDocument(doc);

    // check result
    index_base::InMemorySegmentReaderPtr inMemSegReader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);

    index::AttributeSegmentReaderPtr attrSegReader = inMemSegReader->GetAttributeSegmentReader("biz30day");
    ASSERT_TRUE(attrSegReader);
    float value;
    bool isNull = false;
    autil::mem_pool::Pool pool;
    auto ctx = attrSegReader->CreateReadContextPtr(&pool);
    ASSERT_TRUE(attrSegReader->Read(0, ctx, (uint8_t*)&value, sizeof(float), isNull));
    ASSERT_EQ((float)10, value);

    attrSegReader = inMemSegReader->GetAttributeSegmentReader("price");
    ASSERT_TRUE(attrSegReader);
    uint32_t priceValue;
    ctx = attrSegReader->CreateReadContextPtr(&pool);
    ASSERT_TRUE(attrSegReader->Read(0, nullptr, (uint8_t*)&priceValue, sizeof(uint32_t), isNull));
    ASSERT_EQ((uint32_t)200, priceValue);
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithVirtualAttribute)
{
    config::IndexPartitionSchemaPtr schema = CreateSchemaWithVirtualAttribute();

    index_base::SegmentData segmentData;
    SingleSegmentWriter segWriter(schema, config::IndexPartitionOptions());
    segWriter.Init(segmentData, std::shared_ptr<framework::SegmentMetrics>(new framework::SegmentMetrics),
                   util::BuildResourceMetricsPtr(), util::CounterMapPtr(), plugin::PluginManagerPtr());
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(schema, false, {}, 0, GET_TEMP_DATA_PATH());

    // make one add doc
    std::string docString = "cmd=add,string1=hello0";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    segWriter.AddDocument(doc);

    // check result
    index_base::InMemorySegmentReaderPtr inMemSegReader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);
    index::AttributeSegmentReaderPtr virAttrSegReader = inMemSegReader->GetAttributeSegmentReader("vir_attr");
    ASSERT_TRUE(virAttrSegReader);
    int64_t virAttrValue;
    bool isNull = false;
    autil::mem_pool::Pool pool;
    auto ctx = virAttrSegReader->CreateReadContextPtr(&pool);
    ASSERT_TRUE(virAttrSegReader->Read(0, ctx, (uint8_t*)&virAttrValue, sizeof(int64_t), isNull));
    ASSERT_EQ((int64_t)100, virAttrValue);
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutAttributeSchema)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string summary = "string1";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, "", summary);

    index_base::SegmentData segmentData;
    SingleSegmentWriter segWriter(schema, config::IndexPartitionOptions());
    segWriter.Init(segmentData, std::shared_ptr<framework::SegmentMetrics>(new framework::SegmentMetrics),
                   util::BuildResourceMetricsPtr(), util::CounterMapPtr(), plugin::PluginManagerPtr());
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(schema, false, {}, 0, GET_TEMP_DATA_PATH());

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    ASSERT_TRUE(segWriter.AddDocument(doc));

    index_base::InMemorySegmentReaderPtr inMemSegReader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);
    CheckInMemorySegmentReader(schema, inMemSegReader);

    // check deletion map related with docCount
    index::InMemDeletionMapReaderPtr inMemDeletionMapReader = inMemSegReader->GetInMemDeletionMapReader();
    ASSERT_FALSE(inMemDeletionMapReader->IsDeleted(0));
    ASSERT_TRUE(inMemDeletionMapReader->IsDeleted(1));
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutPkSchema)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "string1:string:string1";
    std::string attribute = "price;biz30day";
    std::string summary = "string1";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, summary);

    index_base::SegmentData segmentData;
    SingleSegmentWriter segWriter(schema, config::IndexPartitionOptions());
    segWriter.Init(segmentData, std::shared_ptr<framework::SegmentMetrics>(new framework::SegmentMetrics),
                   util::BuildResourceMetricsPtr(), util::CounterMapPtr(), plugin::PluginManagerPtr());
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(schema, false, {}, 0, GET_TEMP_DATA_PATH());

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    ASSERT_TRUE(segWriter.AddDocument(doc));

    index_base::InMemorySegmentReaderPtr inMemSegReader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);
    CheckInMemorySegmentReader(schema, inMemSegReader);
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutSummarySchema)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string attribute = "price;biz30day";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, "");

    index_base::SegmentData segmentData;
    SingleSegmentWriter segWriter(schema, config::IndexPartitionOptions());
    segWriter.Init(segmentData, std::shared_ptr<framework::SegmentMetrics>(new framework::SegmentMetrics),
                   util::BuildResourceMetricsPtr(), util::CounterMapPtr(), plugin::PluginManagerPtr());
    std::unique_ptr<NormalDocIdManager> manager =
        DocIdManagerTestUtil::CreateNormalDocIdManager(schema, false, {}, 0, GET_TEMP_DATA_PATH());

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    ASSERT_TRUE(segWriter.AddDocument(doc));

    index_base::InMemorySegmentReaderPtr inMemSegReader = segWriter.CreateInMemSegmentReader();
    ASSERT_TRUE(inMemSegReader);
    CheckInMemorySegmentReader(schema, inMemSegReader);
}

TEST_F(NormalDocIdManagerTest, TestAddDocumentWithoutAttributeDoc)
{
    std::string field = "string1:string;price:uint32;biz30day:float";
    std::string index = "pk:primarykey64:string1";
    std::string attribute = "price;biz30day";
    std::string summary = "string1";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, summary);

    NormalDocIdManager manager(schema, /*enableAutoAdd2Update=*/false);

    // make one add doc
    std::string docString = "cmd=add,string1=hello0,price=200";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    doc->SetAttributeDocument(document::AttributeDocumentPtr());
    ASSERT_FALSE(manager.ValidateAddDocument(doc));
}

}} // namespace indexlib::partition
