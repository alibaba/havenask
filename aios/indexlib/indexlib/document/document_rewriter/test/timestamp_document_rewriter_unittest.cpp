#include "indexlib/document/document_rewriter/test/timestamp_document_rewriter_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index_define.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, TimestampDocumentRewriterTest);

TimestampDocumentRewriterTest::TimestampDocumentRewriterTest()
{
}

TimestampDocumentRewriterTest::~TimestampDocumentRewriterTest()
{
}

void TimestampDocumentRewriterTest::CaseSetUp()
{
}

void TimestampDocumentRewriterTest::CaseTearDown()
{
}

void TimestampDocumentRewriterTest::TestRewrite()
{
    fieldid_t timestampFieldId = 1;
    TimestampDocumentRewriter rewriter(timestampFieldId);
    NormalDocumentPtr document(new NormalDocument);
    IndexDocumentPtr indexDoc(new IndexDocument(document->GetPool()));
    document->SetTimestamp(10);
    document->SetIndexDocument(indexDoc);

    // not add document, do nothing
    rewriter.Rewrite(document);
    ASSERT_FALSE(indexDoc->GetField(timestampFieldId));

    // normal case
    document->SetDocOperateType(ADD_DOC);
    rewriter.Rewrite(document);

    IndexTokenizeField* field = dynamic_cast<IndexTokenizeField*>(indexDoc->GetField(timestampFieldId));
    ASSERT_TRUE(field);
    ASSERT_EQ((size_t)1, field->GetSectionCount());

    Section* section = field->GetSection(0);
    ASSERT_TRUE(section);
    ASSERT_EQ((size_t)1, section->GetTokenCount());

    Token* token = section->GetToken(0);
    ASSERT_TRUE(token);
    ASSERT_EQ((uint64_t)10, token->GetHashKey());
}

void TimestampDocumentRewriterTest::TestRewriteDocAlreadyHasTimestampField()
{
    IndexPartitionSchemaPtr schema = test::SchemaMaker::MakeSchema(
            "string1:string;", "index1:string:string1;", "", "");

    fieldid_t tsFieldId = 1;
    TimestampDocumentRewriter rewriter(tsFieldId);
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(schema,
            "cmd=add,string1=1,ts=1,long1=0;");

    rewriter.Rewrite(doc);
    IndexTokenizeField* tsField = dynamic_cast<IndexTokenizeField*>(doc->GetIndexDocument()->GetField(tsFieldId));
    ASSERT_EQ((size_t)1, tsField->GetSectionCount());
    rewriter.Rewrite(doc);
    ASSERT_EQ((size_t)1, tsField->GetSectionCount());
}

IE_NAMESPACE_END(document);

