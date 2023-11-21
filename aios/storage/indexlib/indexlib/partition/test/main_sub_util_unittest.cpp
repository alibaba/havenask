#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/document.h"
#include "indexlib/partition/main_sub_util.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class MainSubUtilTest : public INDEXLIB_TESTBASE_BASE, public testing::TestWithParam<bool>
{
public:
    MainSubUtilTest() {}
    ~MainSubUtilTest() {}

    DECLARE_CLASS_NAME(MainSubUtilTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

private:
    // Create documents with main doc only, vector<DocOperateType, pk, price>. Refer to schema for details.
    std::vector<document::DocumentPtr>
    CreateMainDocuments(std::vector<std::tuple<DocOperateType, std::string, int>> input);
    // Create documents with arbitrary main and sub doc data.
    std::vector<document::DocumentPtr> CreateDocuments(std::vector<std::string> docStrings);
    bool TestEqual(const std::vector<document::DocumentPtr>& docs,
                   const std::vector<document::DocumentPtr>& expectedDocs);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, MainSubUtilTest);

std::vector<document::DocumentPtr>
MainSubUtilTest::CreateMainDocuments(std::vector<std::tuple<DocOperateType, std::string, int>> input)
{
    std::string field = "price:int32;pk:int64;";
    std::string index = "pk:primarykey64:pk;";
    std::string attr = "price;pk";
    std::string summary = "";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    std::string subField = "substr:string;subpkstr:string;sub_long:uint32;";
    std::string subIndex = "sub_pk:primarykey64:subpkstr";
    std::string subAttr = "substr;subpkstr;sub_long;";
    std::string subSummary = "";
    config::IndexPartitionSchemaPtr subSchema = test::SchemaMaker::MakeSchema(subField, subIndex, subAttr, subSummary);
    schema->SetSubIndexPartitionSchema(subSchema);

    std::vector<document::DocumentPtr> docs;
    for (const auto& tuple : input) {
        std::string docStr = "cmd=" + test::DocumentCreator::GetDocumentTypeStr(std::get<0>(tuple)) +
                             ",pk=" + std::get<1>(tuple) + ",price=" + autil::StringUtil::toString(std::get<2>(tuple));
        document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docStr);
        docs.push_back(doc);
    }
    return docs;
}

std::vector<document::DocumentPtr> MainSubUtilTest::CreateDocuments(std::vector<std::string> docStrings)
{
    std::string field = "price:int32;pk:int64;";
    std::string index = "pk:primarykey64:pk;";
    std::string attr = "price;pk";
    std::string summary = "";
    config::IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(field, index, attr, summary);
    std::string subField = "substr:string;subpkstr:string;sub_long:uint32;";
    std::string subIndex = "sub_pk:primarykey64:subpkstr";
    std::string subAttr = "substr;subpkstr;sub_long;";
    std::string subSummary = "";
    config::IndexPartitionSchemaPtr subSchema = test::SchemaMaker::MakeSchema(subField, subIndex, subAttr, subSummary);
    schema->SetSubIndexPartitionSchema(subSchema);

    std::vector<document::DocumentPtr> docs;
    for (const std::string& docStr : docStrings) {
        document::NormalDocumentPtr doc = test::DocumentCreator::CreateNormalDocument(schema, docStr);
        docs.push_back(doc);
    }
    return docs;
}

bool MainSubUtilTest::TestEqual(const std::vector<document::DocumentPtr>& docs,
                                const std::vector<document::DocumentPtr>& expectedDocs)
{
    if (docs.size() != expectedDocs.size()) {
        return false;
    }
    for (size_t i = 0; i < docs.size(); ++i) {
        document::NormalDocumentPtr doc = std::dynamic_pointer_cast<document::NormalDocument>(docs[i]);
        document::NormalDocumentPtr expectedDoc = std::dynamic_pointer_cast<document::NormalDocument>(docs[i]);
        if (*doc != *expectedDoc) {
            return false;
        }
    }
    return true;
}

TEST_P(MainSubUtilTest, MainOnly_OnePK_AAD)
{
    std::vector<document::DocumentPtr> docs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/200},
                                                                   {ADD_DOC, /*pk=*/"1", /*price=*/120},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/123}});

    std::vector<document::DocumentPtr> expectedDocs = CreateMainDocuments({{DELETE_DOC, /*pk=*/"1", /*price=*/123}});
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs, /*hasSub=*/GetParam());
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_P(MainSubUtilTest, MainOnly_OnePK_ADAD)
{
    std::vector<document::DocumentPtr> docs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/200},
                                                                   {ADD_DOC, /*pk=*/"1", /*price=*/300},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/400}});
    std::vector<document::DocumentPtr> expectedDocs = CreateMainDocuments({{DELETE_DOC, /*pk=*/"1", /*price=*/400}});
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/GetParam());
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_P(MainSubUtilTest, MainOnly_OnePK_ADA)
{
    std::vector<document::DocumentPtr> docs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/120},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/200},
                                                                   {ADD_DOC, /*pk=*/"1", /*price=*/300},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/350},
                                                                   {UPDATE_FIELD, /*pk=*/"1", 360}});
    std::vector<document::DocumentPtr> expectedDocs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/300},
                                                                           {UPDATE_FIELD, /*pk=*/"1", /*price=*/350},
                                                                           {UPDATE_FIELD, /*pk=*/"1", /*price=*/360}});
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/GetParam());
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_P(MainSubUtilTest, MainOnly_OnePK_DDA)
{
    std::vector<document::DocumentPtr> docs = CreateMainDocuments({{DELETE_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/120},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/140},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/200},
                                                                   {ADD_DOC, /*pk=*/"1", /*price=*/300},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/350},
                                                                   {UPDATE_FIELD, /*pk=*/"1", 360}});
    std::vector<document::DocumentPtr> expectedDocs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/300},
                                                                           {UPDATE_FIELD, /*pk=*/"1", /*price=*/350},
                                                                           {UPDATE_FIELD, /*pk=*/"1", /*price=*/360}});
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/GetParam());
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_P(MainSubUtilTest, MainOnly_OnePK_ADD)
{
    std::vector<document::DocumentPtr> docs = CreateMainDocuments({{ADD_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/120},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/100},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/140},
                                                                   {DELETE_DOC, /*pk=*/"1", /*price=*/200},
                                                                   {UPDATE_FIELD, /*pk=*/"1", /*price=*/350},
                                                                   {UPDATE_FIELD, /*pk=*/"1", 360}});
    std::vector<document::DocumentPtr> expectedDocs = CreateMainDocuments({{DELETE_DOC, /*pk=*/"1", /*price=*/300}});
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/GetParam());
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_F(MainSubUtilTest, MainSub_OnePK_DDsub)
{
    std::vector<document::DocumentPtr> docs = CreateDocuments({
        "cmd=update_field,pk=1,price=200,subpkstr=sub_1,sub_long=1010",
        "cmd=delete,pk=1",
        "cmd=update_field,pk=1,price=200,subpkstr=sub_2,sub_long=1099",
        "cmd=delete_sub,pk=1,subpkstr=sub_1,sub_long=1000",
        "cmd=update_field,pk=1,price=200",
    });
    std::vector<document::DocumentPtr> expectedDocs = CreateDocuments({
        "cmd=delete,pk=1",
    });
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/true);
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_F(MainSubUtilTest, MainSub_OnePK_DsubD)
{
    std::vector<document::DocumentPtr> docs = CreateDocuments({
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1123",
        "cmd=delete_sub,pk=1,subpkstr=sub_1,sub_long=1000",
        "cmd=update_field,pk=1,subpkstr=sub_2,sub_long=1099",
        "cmd=delete,pk=1",
        "cmd=update_field,pk=1,price=100,subpkstr=sub_3,sub_long=1456",
    });
    std::vector<document::DocumentPtr> expectedDocs = CreateDocuments({
        "cmd=delete,pk=1",
    });
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/true);
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_F(MainSubUtilTest, MainSub_OnePK_DsubA)
{
    std::vector<document::DocumentPtr> docs = CreateDocuments({
        "cmd=update_field,pk=1,price=200,subpkstr=sub_1,sub_long=1099",
        "cmd=delete_sub,pk=1,subpkstr=sub_1,sub_long=1000",
        "cmd=update_field,pk=1,price=123,subpkstr=sub_2,sub_long=1123",
        "cmd=add,pk=1,price=100,subpkstr=sub_1,sub_long=1000,subpkstr=sub_2,sub_long=1001,subpkstr=sub_3,sub_long=1002",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_2,sub_long=1456",
    });
    std::vector<document::DocumentPtr> expectedDocs = CreateDocuments({
        "cmd=add,pk=1,price=100,subpkstr=sub_1,sub_long=1000,subpkstr=sub_2,sub_long=1001,subpkstr="
        "sub_3,sub_long=1002",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_2,sub_long=1456",
    });
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/true);
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

TEST_F(MainSubUtilTest, MainSub_OnePK_ADsub)
{
    std::vector<document::DocumentPtr> docs = CreateDocuments({
        "cmd=update_field,pk=1,price=200,subpkstr=sub_2,sub_long=1456",
        "cmd=add,pk=1,price=100,subpkstr=sub_1,sub_long=1000,subpkstr=sub_2,sub_long=1001,subpkstr=sub_3,sub_long=1002",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1123,subpkstr=sub_2,sub_long=2123",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_2,sub_long=1124,subpkstr=sub_3,sub_long=2124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1125,subpkstr=sub_1,sub_long=2125",
        "cmd=delete_sub,pk=1,subpkstr=sub_1",
        "cmd=update_field,pk=1,price=301,subpkstr=sub_1,sub_long=1223",
        "cmd=update_field,pk=1,price=401,subpkstr=sub_2,sub_long=1224",
        "cmd=update_field,pk=1,price=501,subpkstr=sub_3,sub_long=1225",
    });
    std::vector<document::DocumentPtr> expectedDocs = CreateDocuments({
        "cmd=add,pk=1,price=100,subpkstr=sub_2,sub_long=1001,subpkstr=sub_3,sub_long=1002",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1123",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_2,sub_long=1124,subpkstr=sub_3,sub_long=2124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1125",
        "cmd=update_field,pk=1,price=301",
        "cmd=update_field,pk=1,price=401,subpkstr=sub_2,sub_long=1224",
        "cmd=update_field,pk=1,price=501,subpkstr=sub_3,sub_long=1225",
    });
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/true);
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

// TODO(panghai.hj): Combine this Dsub cases and return one Dsub doc.
TEST_F(MainSubUtilTest, MainSub_OnePK_DsubDsub)
{
    std::vector<document::DocumentPtr> docs = CreateDocuments({
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1123,subpkstr=sub_2,sub_long=2123",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_2,sub_long=1124,subpkstr=sub_3,sub_long=2124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1125,subpkstr=sub_1,sub_long=2125",
        "cmd=delete_sub,pk=1,subpkstr=sub_1,sub_long=1000",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1223",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_2,sub_long=1224",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1225",
        "cmd=delete_sub,pk=1,subpkstr=sub_2,sub_long=1001",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_1,sub_long=1323,subpkstr=sub_3,sub_long=3123",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_2,sub_long=1324,subpkstr=sub_3,sub_long=3124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1325",
        "cmd=delete_sub,pk=1,subpkstr=sub_2,sub_long=1001",
    });
    std::vector<document::DocumentPtr> expectedDocs = CreateDocuments({
        "cmd=update_field,pk=1,price=300",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_3,sub_long=2124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1125",
        "cmd=delete_sub,pk=1,subpkstr=sub_1,sub_long=1000",
        "cmd=update_field,pk=1,price=300",
        "cmd=update_field,pk=1,price=400",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1225",
        "cmd=update_field,pk=1,price=300,subpkstr=sub_3,sub_long=3123",
        "cmd=update_field,pk=1,price=400,subpkstr=sub_3,sub_long=3124",
        "cmd=update_field,pk=1,price=500,subpkstr=sub_3,sub_long=1325",
        "cmd=delete_sub,pk=1,subpkstr=sub_2,sub_long=1001",
    });
    std::vector<document::DocumentPtr> actualDocs = MainSubUtil::FilterDocsForBatch(docs,
                                                                                    /*hasSub=*/true);
    EXPECT_TRUE(TestEqual(actualDocs, expectedDocs));
}

// INSTANTIATE_TEST_SUITE_P(HasSubDoc, MainSubUtilTest, testing::Bool());
// INSTANTIATE_TEST_CASE_P(HasSubDoc, MainSubUtilTest, testing::Bool());
INSTANTIATE_TEST_CASE_P(HasSubDoc, MainSubUtilTest, testing::Values(true));

}} // namespace indexlib::partition
