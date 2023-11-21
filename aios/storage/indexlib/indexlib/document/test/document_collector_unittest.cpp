#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace document {

class DocumentCollectorTest : public INDEXLIB_TESTBASE
{
public:
    DocumentCollectorTest() {}

    ~DocumentCollectorTest() {}

    DECLARE_CLASS_NAME(DocumentCollectorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

public:
    static document::NormalDocumentPtr CreateUpdateDocument(config::IndexPartitionSchemaPtr schema,
                                                            std::map<fieldid_t, int> fieldIdToNumTokens);
};

document::NormalDocumentPtr DocumentCollectorTest::CreateUpdateDocument(config::IndexPartitionSchemaPtr schema,
                                                                        std::map<fieldid_t, int> fieldIdToNumTokens)
{
    static int pk = 123456;
    std::string docStr =
        "cmd=" + test::DocumentCreator::GetDocumentTypeStr(UPDATE_FIELD) + ",pk=" + autil::StringUtil::toString(pk);
    pk++;
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docStr);
    std::vector<ModifiedTokens> multiModifiedTokens;
    for (const auto& pair : fieldIdToNumTokens) {
        std::vector<int64_t> termKeys;
        for (int i = 0; i < pair.second; ++i) {
            docid_t docId = rand() % 100;
            termKeys.push_back(docId % 2 == 0 ? docId : -docId);
        }
        document::ModifiedTokens tokens = ModifiedTokens::TEST_CreateModifiedTokens(/*fieldId=*/pair.first, termKeys);
        multiModifiedTokens.push_back(tokens);
    }
    doc->GetIndexDocument()->SetModifiedTokens(multiModifiedTokens);
    return doc;
}

TEST_F(DocumentCollectorTest, TestBatchStatistics)
{
    std::string field = "pk:string;field1:int64;field2:int64;field3:int64";
    std::string index = "pk:primarykey64:pk;field1:number:field1;field2:number:field2;field3:number:field3";
    std::string attribute = "pk;field1;field2;field3";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attribute, "");
    schema->GetIndexSchema()->GetIndexConfig("field1")->TEST_SetIndexUpdatable(true);
    schema->GetIndexSchema()->GetIndexConfig("field2")->TEST_SetIndexUpdatable(false);
    schema->GetIndexSchema()->GetIndexConfig("field3")->TEST_SetIndexUpdatable(true);

    std::vector<document::DocumentPtr> documents;
    for (int i = 0; i < 10; i++) {
        const int baseValue = i * 10;
        std::string docStr = "cmd=" + test::DocumentCreator::GetDocumentTypeStr(ADD_DOC) +
                             ",pk=" + autil::StringUtil::toString(i) +
                             ",field1=" + autil::StringUtil::toString(baseValue + 1) +
                             ",field2=" + autil::StringUtil::toString(baseValue + 2) +
                             ",field3=" + autil::StringUtil::toString(baseValue + 3);
        document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docStr);
        documents.push_back(doc);
    }
    documents.push_back(nullptr);
    for (int i = 0; i < 10; i++) {
        document::NormalDocumentPtr doc = CreateUpdateDocument(
            schema, /*fieldIdToNumTokens=*/ {
                {/*fieldId=*/1, /*numTokens=*/1}, {/*fieldId=*/2, /*numTokens=*/2}, {/*fieldId=*/3, /*numTokens=*/3}});
        documents.push_back(doc);
    }
    std::map<std::string, int> statistics = DocumentCollector::GetBatchStatistics(documents, schema);
    EXPECT_EQ(8, statistics.size());
    EXPECT_EQ(10, statistics["add"]);
    EXPECT_EQ(10, statistics["update"]);
    EXPECT_EQ(0, statistics["delete"]);
    EXPECT_EQ(0, statistics["other"]);
    EXPECT_EQ(1, statistics["failed"]);
    EXPECT_EQ(10, statistics["index_field1"]);
    EXPECT_EQ(0, statistics["index_field2"]);
    EXPECT_EQ(30, statistics["index_field3"]);
}

}} // namespace indexlib::document
