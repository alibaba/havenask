#include "indexlib/document/document_rewriter/test/document_rewriter_creator_unittest.h"

#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DocumentRewriterCreatorTest);

DocumentRewriterCreatorTest::DocumentRewriterCreatorTest() {}

DocumentRewriterCreatorTest::~DocumentRewriterCreatorTest() {}

void DocumentRewriterCreatorTest::CaseSetUp()
{
    mSchema = SchemaMaker::MakeSchema("string0:string;price:int32;score:double", // Field schema
                                      "pk:primarykey64:string0;",                // Index schema
                                      "string0;price;score",                     // attribute
                                      "");
}

void DocumentRewriterCreatorTest::CaseTearDown() {}

void DocumentRewriterCreatorTest::TestCreateTimestampRewriter()
{
    DocumentRewriterPtr rewriter(DocumentRewriterCreator::CreateTimestampRewriter(mSchema));
    ASSERT_TRUE(!rewriter);

    IndexPartitionSchemaPtr rtSchema(mSchema->Clone());
    SchemaLoader::RewriteToRtSchema(rtSchema);
    rewriter.reset(DocumentRewriterCreator::CreateTimestampRewriter(rtSchema));
    ASSERT_TRUE(rewriter);
}

void DocumentRewriterCreatorTest::TestCreateAdd2UpdateRewriter()
{
    IndexPartitionOptions options;
    indexlibv2::config::SortDescriptions partMeta;
    DocumentRewriterPtr rewriter(
        DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(mSchema, options, partMeta));
    ASSERT_TRUE(rewriter);

    // test no pk
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string0:string;price:int32",  // Field schema
                                                             "stringIndex:string:string0;", // Index schema
                                                             "string0",                     // attribute
                                                             "");
    rewriter.reset(DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(schema, options, partMeta));
    ASSERT_FALSE(rewriter);

    // test no attribute
    schema = SchemaMaker::MakeSchema("string0:string;price:int32", // Field schema
                                     "pk:primarykey64:string0;",   // Index schema
                                     "", "");
    rewriter.reset(DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(schema, options, partMeta));
    ASSERT_FALSE(rewriter);
}

void DocumentRewriterCreatorTest::TestCreateAdd2UpdateRewriterWithMultiConfigs()
{
    vector<IndexPartitionOptions> optionVec;
    optionVec.resize(2);

    vector<indexlibv2::config::SortDescriptions> partMetaVec;
    partMetaVec.resize(2);
    partMetaVec[0].push_back(indexlibv2::config::SortDescription("price", indexlibv2::config::sp_desc));
    partMetaVec[1].push_back(indexlibv2::config::SortDescription("price", indexlibv2::config::sp_desc));
    partMetaVec[1].push_back(indexlibv2::config::SortDescription("score", indexlibv2::config::sp_asc));

    DocumentRewriterPtr rewriter(
        DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(mSchema, optionVec, partMetaVec));
    ASSERT_TRUE(rewriter);

    AddToUpdateDocumentRewriterPtr singleRewriter = DYNAMIC_POINTER_CAST(AddToUpdateDocumentRewriter, rewriter);
    ASSERT_TRUE(singleRewriter);

    ASSERT_FALSE(singleRewriter->mAttrUpdatableFieldIds.Test(0));
    ASSERT_FALSE(singleRewriter->mAttrUpdatableFieldIds.Test(1));
    ASSERT_FALSE(singleRewriter->mAttrUpdatableFieldIds.Test(2));

    // test optionVec.size() not equal with partMetaVec
    optionVec.resize(3);
    ASSERT_ANY_THROW(DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(mSchema, optionVec, partMetaVec));
}

void DocumentRewriterCreatorTest::TestCreateHashIdDocumentRewriter()
{
    IndexPartitionSchemaPtr schema(mSchema->Clone());
    schema->SetEnableHashId(true);
    {
        DocumentRewriterPtr rewriter(DocumentRewriterCreator::CreateHashIdDocumentRewriter(mSchema));
        ASSERT_FALSE(rewriter);
    }
    {
        DocumentRewriterPtr rewriter(DocumentRewriterCreator::CreateHashIdDocumentRewriter(schema));
        ASSERT_TRUE(rewriter);
    }
}
}} // namespace indexlib::document
