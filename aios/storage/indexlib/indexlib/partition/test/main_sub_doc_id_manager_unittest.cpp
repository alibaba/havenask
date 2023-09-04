#include <string>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/primarykey/composite_primary_key_reader.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_info_creator.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/doc_id_manager.h"
#include "indexlib/partition/main_sub_doc_id_manager.h"
#include "indexlib/partition/test/doc_id_manager_test_util.h"
#include "indexlib/partition/test/main_sub_test_util.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace partition {

class MockCompositePrimaryKeyReader : public index::CompositePrimaryKeyReader
{
public:
    MockCompositePrimaryKeyReader() {}

public:
    MOCK_METHOD(docid_t, Lookup, (const std::string& pkStr), (const, override));
};

class MainSubDocIdManagerTest : public INDEXLIB_TESTBASE
{
public:
    MainSubDocIdManagerTest() {}

    DECLARE_CLASS_NAME(MainSubDocIdManagerTest);
    void CaseSetUp() override
    {
        _schema = test::SchemaMaker::MakeSchema("pk:int64;price:int32;", /*index=*/"pk:primarykey64:pk;",
                                                /*attr=*/"pk;price", /*summary=*/"");
        _schema->SetSubIndexPartitionSchema(test::SchemaMaker::MakeSchema("sub_pk:int64;sub_price:int32;",
                                                                          /*index=*/"sub_pk:primarykey64:sub_pk;",
                                                                          /*attr=*/"sub_pk;sub_price", /*summary=*/""));
        index_base::SchemaRewriter::RewriteForSubTable(_schema);
    }
    void CaseTearDown() override {}

private:
    void Process(MainSubDocIdManager* manager, DocOperateType op, int32_t pk, const std::vector<int32_t>& subPks,
                 bool expectSuccess, docid_t expectMainDocId, const std::vector<docid_t>& expectSubDocIds);
    void CheckJoinFieldInDoc(const document::NormalDocumentPtr& doc, docid_t mainJoinValue, docid_t subJoinValue);
    void RemoveAttributeDocument(document::NormalDocumentPtr& doc);
    index::JoinDocidAttributeReaderPtr CreateJoinDocidAttributeReader(std::string mainInc, std::string subInc);

private:
    config::IndexPartitionSchemaPtr _schema;

private:
    IE_LOG_DECLARE();
};

TEST_F(MainSubDocIdManagerTest, TestDedupSubDoc)
{
    std::string field = "pkstr:string;text1:text;long1:uint32;";
    std::string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    std::string attr = "long1;";
    std::string summary = "pkstr;";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    config::IndexPartitionSchemaPtr subSchema = test::SchemaMaker::MakeSchema(
        "substr1:string;subpkstr:string;sub_long:uint32;", "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
        "substr1;subpkstr;sub_long;", "");
    schema->SetSubIndexPartitionSchema(subSchema);
    index_base::SchemaRewriter::RewriteForSubTable(schema);

    MainSubDocIdManager docIdManager(schema);
    std::string docString = "cmd=add,pkstr=2,subpkstr=1^2^1^3^4^3";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docString);
    docIdManager.DedupSubDocs(doc);
    const document::NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((size_t)4, subDocs.size());
    ASSERT_EQ(std::string("2"), subDocs[0]->GetPrimaryKey());
    ASSERT_EQ(std::string("1"), subDocs[1]->GetPrimaryKey());
    ASSERT_EQ(std::string("4"), subDocs[2]->GetPrimaryKey());
    ASSERT_EQ(std::string("3"), subDocs[3]->GetPrimaryKey());
}

TEST_F(MainSubDocIdManagerTest, TestNeedUpdate)
{
    std::string field = "price:int32;pk:int64";
    std::string index = "pk:primarykey64:pk";
    std::string attr = "price;pk";
    std::string summary = "";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    config::IndexPartitionSchemaPtr subSchema = test::SchemaMaker::MakeSchema(
        "sub_pk:string;sub_long:uint32;", "sub_pk:primarykey64:sub_pk", "sub_pk;sub_long;", "");

    schema->SetSubIndexPartitionSchema(subSchema);
    index_base::SchemaRewriter::RewriteForSubTable(schema);

    fieldid_t pkFieldId = schema->GetIndexSchema()->GetPrimaryKeyIndexFieldId();

    // empty
    std::vector<fieldid_t> fieldIds;
    std::vector<std::string> fieldValues;
    document::NormalDocumentPtr doc = index::DocumentMaker::MakeUpdateDocument(schema, "1", fieldIds, fieldValues, 10);
    ASSERT_FALSE(MainSubDocIdManager::NeedUpdate(doc, pkFieldId));

    // pk attribute only
    fieldIds.push_back(pkFieldId);
    fieldValues.push_back("1");
    doc = index::DocumentMaker::MakeUpdateDocument(schema, "1", fieldIds, fieldValues, 10);
    ASSERT_FALSE(MainSubDocIdManager::NeedUpdate(doc, pkFieldId));

    // has attribute field and pkAttr
    fieldIds.push_back(schema->GetFieldId("price"));
    fieldValues.push_back("111");
    doc = index::DocumentMaker::MakeUpdateDocument(schema, "1", fieldIds, fieldValues, 10);
    ASSERT_TRUE(MainSubDocIdManager::NeedUpdate(doc, pkFieldId));

    // no attribute document
    doc->SetAttributeDocument(document::AttributeDocumentPtr());
    ASSERT_FALSE(MainSubDocIdManager::NeedUpdate(doc, pkFieldId));
}

TEST_F(MainSubDocIdManagerTest, TestValidateSubDocsIfMainDocIdInvalid)
{
    auto mockMainPkIndexReader = std::make_unique<MockCompositePrimaryKeyReader>();
    EXPECT_CALL(*mockMainPkIndexReader, Lookup(_)).WillOnce(Return(INVALID_DOCID));

    document::NormalDocumentPtr doc = index::DocumentMaker::MakeDeletionDocument("10", 100);
    document::NormalDocumentPtr subDoc1 = index::DocumentMaker::MakeDeletionDocument("101", 100);
    doc->AddSubDocument(subDoc1);
    ASSERT_FALSE(MainSubDocIdManager::ValidateAndCorrectMainSubRelation(doc, mockMainPkIndexReader.get(),
                                                                        /*subPrimaryKeyReader=*/nullptr,
                                                                        /*joinDocIdAttributeReader=*/nullptr));
}

TEST_F(MainSubDocIdManagerTest, TestValidateSubDocs)
{
    auto mockMainPkIndexReader = std::make_unique<MockCompositePrimaryKeyReader>();
    EXPECT_CALL(*mockMainPkIndexReader, Lookup(_)).WillOnce(Return(0));

    auto mockSubPkIndexReader = std::make_unique<MockCompositePrimaryKeyReader>();
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("101")).WillOnce(Return(INVALID_DOCID));
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("102")).WillOnce(Return(1));
    EXPECT_CALL(*mockSubPkIndexReader, Lookup("103")).WillOnce(Return(10));

    // Main 0 ~ Sub[0,2)
    index::JoinDocidAttributeReaderPtr reader = CreateJoinDocidAttributeReader("2", "0,0");
    // make it has 3 sub doc: 101(INVALID_DOCID,ok), 102(1,ok), 103(10,remove)
    document::NormalDocumentPtr doc = index::DocumentMaker::MakeDeletionDocument("10", 100);
    document::NormalDocumentPtr subDoc1 = index::DocumentMaker::MakeDeletionDocument("101", 100);
    doc->AddSubDocument(subDoc1);
    document::NormalDocumentPtr subDoc2 = index::DocumentMaker::MakeDeletionDocument("102", 100);
    doc->AddSubDocument(subDoc2);
    document::NormalDocumentPtr subDoc3 = index::DocumentMaker::MakeDeletionDocument("103", 100);
    doc->AddSubDocument(subDoc3);

    ASSERT_TRUE(MainSubDocIdManager::ValidateAndCorrectMainSubRelation(doc, mockMainPkIndexReader.get(),
                                                                       mockSubPkIndexReader.get(), reader.get()));
    ASSERT_EQ((size_t)1, doc->GetSubDocuments().size());
}

void MainSubDocIdManagerTest::CheckJoinFieldInDoc(const document::NormalDocumentPtr& doc, docid_t mainJoinValue,
                                                  docid_t subJoinValue)
{
    config::IndexPartitionSchemaPtr subSchema = _schema->GetSubIndexPartitionSchema();
    fieldid_t mainFieldId = _schema->GetFieldId(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    fieldid_t subFieldId = subSchema->GetFieldId(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);

    autil::StringView fieldValue = doc->GetAttributeDocument()->GetField(mainFieldId);
    ASSERT_EQ(mainJoinValue, *(docid_t*)fieldValue.data());

    document::NormalDocument::DocumentVector subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); i++) {
        autil::StringView subFieldValue = subDocs[i]->GetAttributeDocument()->GetField(subFieldId);
        ASSERT_EQ(subJoinValue, *(docid_t*)subFieldValue.data());
    }
}

void MainSubDocIdManagerTest::RemoveAttributeDocument(document::NormalDocumentPtr& doc)
{
    doc->SetAttributeDocument(document::AttributeDocumentPtr());
    document::NormalDocument::DocumentVector subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); i++) {
        subDocs[i]->SetAttributeDocument(document::AttributeDocumentPtr());
    }
}

TEST_F(MainSubDocIdManagerTest, TestAddJoinFieldToDocumentWithoutAttribute)
{
    std::unique_ptr<MainSubDocIdManager> manager =
        DocIdManagerTestUtil::CreateMainSubDocIdManager(_schema, false, {}, 0, 0, GET_TEMP_DATA_PATH());

    std::string docString = "cmd=add,pk=2,sub_pk=1^2";

    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, docString, true);
    RemoveAttributeDocument(doc);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 2, 0);

    doc = test::DocumentCreator::CreateNormalDocument(_schema, docString, true);
    RemoveAttributeDocument(doc);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 4, 1);

    doc = test::DocumentCreator::CreateNormalDocument(_schema, docString, true);
    RemoveAttributeDocument(doc);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 6, 2);
}

TEST_F(MainSubDocIdManagerTest, TestAddJoinFieldToDocumentWithAttribute)
{
    std::unique_ptr<MainSubDocIdManager> manager =
        DocIdManagerTestUtil::CreateMainSubDocIdManager(_schema, false, {}, 0, 0, GET_TEMP_DATA_PATH());

    std::string docString = "cmd=add,pk=2,price=3,sub_pk=1^2,sub_price=4^5";

    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, docString);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 2, 0);

    doc = test::DocumentCreator::CreateNormalDocument(_schema, docString);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 4, 1);

    doc = test::DocumentCreator::CreateNormalDocument(_schema, docString);
    manager->Process(doc);
    CheckJoinFieldInDoc(doc, 6, 2);
}

index::JoinDocidAttributeReaderPtr MainSubDocIdManagerTest::CreateJoinDocidAttributeReader(std::string mainInc,
                                                                                           std::string subInc)
{
    std::string mainRoot = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "main");
    if (file_system::FSEC_OK != file_system::FslibWrapper::MkDir(mainRoot).Code()) {
        return nullptr;
    }

    test::SingleFieldPartitionDataProviderPtr mainProvider(new test::SingleFieldPartitionDataProvider);
    mainProvider->Init(mainRoot, "int32", test::SFP_ATTRIBUTE);
    mainProvider->Build(mainInc, test::SFP_OFFLINE);
    index_base::PartitionDataPtr partData = mainProvider->GetPartitionData();
    config::AttributeConfigPtr attrConfig = mainProvider->GetAttributeConfig();
    index::JoinDocidAttributeReaderPtr reader(new index::JoinDocidAttributeReader);
    reader->Open(attrConfig, partData, index::PatchApplyStrategy::PAS_APPLY_NO_PATCH);

    auto subRoot = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "sub");
    if (file_system::FSEC_OK != file_system::FslibWrapper::MkDir(subRoot).Code()) {
        return nullptr;
    }

    test::SingleFieldPartitionDataProviderPtr subProvider(new test::SingleFieldPartitionDataProvider);
    subProvider->Init(subRoot, "int32", test::SFP_ATTRIBUTE);
    subProvider->Build(subInc, test::SFP_OFFLINE);
    index_base::PartitionDataPtr subPartData = subProvider->GetPartitionData();
    reader->InitJoinBaseDocId(subPartData);

    return reader;
}

void MainSubDocIdManagerTest::Process(MainSubDocIdManager* manager, DocOperateType op, int32_t pk,
                                      const std::vector<int32_t>& subPks, bool expectSuccess, docid_t expectMainDocId,
                                      const std::vector<docid_t>& expectSubDocIds)
{
    std::stringstream ss;
    ss << "cmd=" << test::DocumentCreator::GetDocumentTypeStr(op) << ",";
    ss << "pk=" << pk << ",price=" << pk << ",";
    ss << "sub_pk=" << autil::StringUtil::toString(subPks, "^") << ",";
    ss << "sub_price=" << autil::StringUtil::toString(subPks, "^") << ",";
    document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(_schema, ss.str());

    std::stringstream ss2;
    ss2 << "Error Process [" << test::DocumentCreator::GetDocumentTypeStr(op);
    ss2 << " " << pk << ": " << autil::StringUtil::toString(subPks, ",") << "]";
    ss2 << " => expect [" << expectMainDocId << ": " << autil::StringUtil::toString(expectSubDocIds, ",") << "]";
    std::string debugString = ss2.str();

    ASSERT_EQ(expectSuccess, manager->Process(doc)) << debugString;

    std::vector<docid_t> actualSubDocIds;
    for (size_t i = 0; i < doc->GetSubDocuments().size(); ++i) {
        actualSubDocIds.push_back(doc->GetSubDocuments()[i]->GetDocId());
    }
    ss2 << ", actually [" << doc->GetDocId() << ": " << autil::StringUtil::toString(actualSubDocIds, ",") << "]";
    debugString = ss2.str();

    ASSERT_EQ(expectMainDocId, doc->GetDocId()) << debugString;
    ASSERT_EQ(expectSubDocIds.size(), doc->GetSubDocuments().size()) << debugString;
    for (size_t i = 0; i < expectSubDocIds.size(); ++i) {
        ASSERT_EQ(expectSubDocIds[i], doc->GetSubDocuments()[i]->GetDocId()) << debugString;
    }
}

TEST_F(MainSubDocIdManagerTest, TestAdd)
{
    std::unique_ptr<MainSubDocIdManager> manager = DocIdManagerTestUtil::CreateMainSubDocIdManager(
        _schema, /*batchMode=*/true, /*mainDocIdToSubDocIds=*/ {{0, {0, 1}}, {1, {2, 3}}},
        /*mainBuildingSegmentBaseDocId=*/2, /*subBuildingSegmentBaseDocId=*/4, GET_TEMP_DATA_PATH());

    Process(manager.get(), ADD_DOC, 2, {4, 5}, /*expect*/ true, 2, {4, 5});
    Process(manager.get(), ADD_DOC, 3, {}, /*expect*/ true, 3, {});
    Process(manager.get(), ADD_DOC, 4, {6}, /*expect*/ true, 4, {6});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {}));

    Process(manager.get(), ADD_DOC, 2, {7, 8}, /*expect*/ true, 5, {7, 8});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {2}, {4, 5}));

    Process(manager.get(), ADD_DOC, 2, {}, /*expect*/ true, 6, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {2, 5}, {4, 5, 7, 8}));

    Process(manager.get(), ADD_DOC, 4, {4}, /*expect*/ true, 7, {9});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {2, 4, 5}, {4, 5, 6, 7, 8}));
}

TEST_F(MainSubDocIdManagerTest, TestAddDupSub)
{
    std::unique_ptr<MainSubDocIdManager> manager = DocIdManagerTestUtil::CreateMainSubDocIdManager(
        _schema, /*batchMode=*/true, /*mainDocIdToSubDocIds=*/ {{0, {0, 1}}, {1, {2, 3}}},
        /*mainBuildingSegmentBaseDocId=*/2, /*subBuildingSegmentBaseDocId=*/4, GET_TEMP_DATA_PATH());

    Process(manager.get(), ADD_DOC, 2, {2, 4}, /*expect*/ true, 2, {4, 5});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {2}));
}

TEST_F(MainSubDocIdManagerTest, TestUpdate)
{
    std::unique_ptr<MainSubDocIdManager> manager = DocIdManagerTestUtil::CreateMainSubDocIdManager(
        _schema, /*batchMode=*/true, /*mainDocIdToSubDocIds=*/ {{0, {0, 1}}, {1, {2, 3}}},
        /*mainBuildingSegmentBaseDocId=*/2, /*subBuildingSegmentBaseDocId=*/4, GET_TEMP_DATA_PATH());

    Process(manager.get(), UPDATE_FIELD, 0, {}, /*expect*/ true, 0, {});
    Process(manager.get(), UPDATE_FIELD, 0, {0}, /*expect*/ true, 0, {0});
    Process(manager.get(), UPDATE_FIELD, 0, {1}, /*expect*/ true, 0, {1});
    Process(manager.get(), UPDATE_FIELD, 0, {2}, /*expect*/ true, 0, {});
    Process(manager.get(), UPDATE_FIELD, 0, {0, 1}, /*expect*/ true, 0, {0, 1});
    Process(manager.get(), UPDATE_FIELD, 0, {0, 2}, /*expect*/ true, 0, {0});
    Process(manager.get(), UPDATE_FIELD, 0, {0, 4}, /*expect*/ true, 0, {0});
    Process(manager.get(), UPDATE_FIELD, 0, {4, 0, 4, 4}, /*expect*/ true, 0, {0});
    Process(manager.get(), UPDATE_FIELD, 0, {1, 0, 1, 1}, /*expect*/ true, 0, {1, 0, 1, 1});
    Process(manager.get(), UPDATE_FIELD, 0, {4, 5}, /*expect*/ true, 0, {});
    Process(manager.get(), UPDATE_FIELD, 0, {5, 0}, /*expect*/ true, 0, {0});

    Process(manager.get(), UPDATE_FIELD, 1, {}, /*expect*/ true, 1, {});
    Process(manager.get(), UPDATE_FIELD, 1, {2}, /*expect*/ true, 1, {2});
    Process(manager.get(), UPDATE_FIELD, 1, {3}, /*expect*/ true, 1, {3});
    Process(manager.get(), UPDATE_FIELD, 1, {1}, /*expect*/ true, 1, {});
    Process(manager.get(), UPDATE_FIELD, 1, {2, 3}, /*expect*/ true, 1, {2, 3});
    Process(manager.get(), UPDATE_FIELD, 1, {2, 4}, /*expect*/ true, 1, {2});
    Process(manager.get(), UPDATE_FIELD, 1, {3, 5}, /*expect*/ true, 1, {3});
    Process(manager.get(), UPDATE_FIELD, 1, {4, 4, 2, 4}, /*expect*/ true, 1, {2});
    Process(manager.get(), UPDATE_FIELD, 1, {2, 2, 3, 2}, /*expect*/ true, 1, {2, 2, 3, 2});
    Process(manager.get(), UPDATE_FIELD, 1, {4, 5}, /*expect*/ true, 1, {});
    Process(manager.get(), UPDATE_FIELD, 1, {5, 3}, /*expect*/ true, 1, {3});

    Process(manager.get(), UPDATE_FIELD, 2, {0}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {1}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {2}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {3}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {4}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {0, 3}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID, INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {2, 5}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID, INVALID_DOCID});
    Process(manager.get(), UPDATE_FIELD, 2, {3, 5}, /*expect*/ false, INVALID_DOCID, {INVALID_DOCID, INVALID_DOCID});

    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {}));
}

TEST_F(MainSubDocIdManagerTest, TestDelete)
{
    std::unique_ptr<MainSubDocIdManager> manager = DocIdManagerTestUtil::CreateMainSubDocIdManager(
        _schema, /*batchMode=*/true,
        /*mainDocIdToSubDocIds=*/
        {{0, {0, 1}}, {1, {2, 3}}, {2, {4, 5}}, {3, {6, 7}}, {4, {8, 9}}, {5, {10, 11}}, {6, {12, 13}}},
        /*mainBuildingSegmentBaseDocId=*/15, /*subBuildingSegmentBaseDocId=*/20, GET_TEMP_DATA_PATH());

    Process(manager.get(), DELETE_DOC, 1, {}, /*expect*/ true, 1, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1}, {2, 3}));

    Process(manager.get(), DELETE_DOC, 2, {2}, /*expect*/ true, 2, {INVALID_DOCID});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2}, {2, 3, 4, 5}));

    Process(manager.get(), DELETE_DOC, 3, {6, 7}, /*expect*/ true, 3, {INVALID_DOCID, INVALID_DOCID});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2, 3}, {2, 3, 4, 5, 6, 7}));

    Process(manager.get(), DELETE_DOC, 4, {10, 11}, /*expect*/ true, 4, {INVALID_DOCID, INVALID_DOCID});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2, 3, 4}, {2, 3, 4, 5, 6, 7, 8, 9}));

    Process(manager.get(), DELETE_DOC, 5, {10, 11}, /*expect*/ true, 5, {INVALID_DOCID, INVALID_DOCID});
    ASSERT_TRUE(
        DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2, 3, 4, 5}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));

    Process(manager.get(), DELETE_DOC, 99, {}, /*expect*/ false, INVALID_DOCID, {});
    ASSERT_TRUE(
        DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2, 3, 4, 5}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));

    Process(manager.get(), DELETE_DOC, 99, {0, 3, 13, 99}, /*expect*/ false, INVALID_DOCID,
            {INVALID_DOCID, INVALID_DOCID, INVALID_DOCID, INVALID_DOCID});
    ASSERT_TRUE(
        DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {1, 2, 3, 4, 5}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));

    Process(manager.get(), DELETE_DOC, 0, {}, /*expect*/ true, 0, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {0, 1, 2, 3, 4, 5},
                                                         {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}));
}

TEST_F(MainSubDocIdManagerTest, TestDeleteSub)
{
    std::unique_ptr<MainSubDocIdManager> manager = DocIdManagerTestUtil::CreateMainSubDocIdManager(
        _schema, /*batchMode=*/true,
        /*mainDocIdToSubDocIds=*/
        {{0, {0, 1}}, {1, {2, 3}}, {2, {4, 5}}, {3, {6, 7}}, {4, {8, 9}}, {5, {10, 11}}, {6, {12, 13}}},
        /*mainBuildingSegmentBaseDocId=*/15, /*subBuildingSegmentBaseDocId=*/20, GET_TEMP_DATA_PATH());

    Process(manager.get(), DELETE_SUB_DOC, 0, {}, /*expect*/ false, 0, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {}));

    Process(manager.get(), DELETE_SUB_DOC, 1, {0}, /*expect*/ false, 1, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {}));

    Process(manager.get(), DELETE_SUB_DOC, 0, {0}, /*expect*/ true, 0, {0});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0}));

    Process(manager.get(), DELETE_SUB_DOC, 1, {2}, /*expect*/ true, 1, {2});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2}));

    Process(manager.get(), DELETE_SUB_DOC, 1, {3}, /*expect*/ true, 1, {3});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3}));

    Process(manager.get(), DELETE_SUB_DOC, 1, {4}, /*expect*/ false, 1, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3}));

    Process(manager.get(), DELETE_SUB_DOC, 2, {4, 5}, /*expect*/ true, 2, {4, 5});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3, 4, 5}));

    Process(manager.get(), DELETE_SUB_DOC, 3, {0, 1, 2, 3, 4, 5, 6}, /*expect*/ true, 3, {6});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3, 4, 5, 6}));

    Process(manager.get(), DELETE_SUB_DOC, 3, {0, 1, 2, 3, 4, 5, 6, 7}, /*expect*/ true, 3, {7});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3, 4, 5, 6, 7}));

    Process(manager.get(), DELETE_SUB_DOC, 4, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, /*expect*/ true, 4, {8, 9});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3, 4, 5, 6, 7, 8, 9}));

    Process(manager.get(), DELETE_SUB_DOC, 4, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, /*expect*/ false, 4, {});
    ASSERT_TRUE(DocIdManagerTestUtil::CheckDeletedDocIds(manager.get(), {}, {0, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST_F(MainSubDocIdManagerTest, TestUpdateDocumentWithNoField)
{
    fieldid_t pkFieldId = _schema->GetIndexSchema()->GetPrimaryKeyIndexFieldId();
    std::vector<fieldid_t> fieldIds(1, pkFieldId);
    std::vector<std::string> fieldValues(1, "1");
    document::DocumentPtr doc = index::DocumentMaker::MakeUpdateDocument(_schema, "1", fieldIds, fieldValues, 10);

    std::unique_ptr<MainSubDocIdManager> manager =
        DocIdManagerTestUtil::CreateMainSubDocIdManager(_schema, false, {}, 0, 0, GET_TEMP_DATA_PATH());
    ASSERT_FALSE(manager->Process(doc));
}

IE_LOG_SETUP(partition, MainSubDocIdManagerTest);
}} // namespace indexlib::partition
