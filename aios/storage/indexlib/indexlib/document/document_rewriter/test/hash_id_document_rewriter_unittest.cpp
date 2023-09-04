#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/document/document_rewriter/hash_id_document_rewriter.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::test;

namespace indexlib { namespace document {

class HashIdDocumentRewriterTest : public INDEXLIB_TESTBASE
{
public:
    HashIdDocumentRewriterTest();
    ~HashIdDocumentRewriterTest();

    DECLARE_CLASS_NAME(HashIdDocumentRewriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashIdDocumentRewriterTest, TestSimpleProcess);
IE_LOG_SETUP(document, HashIdDocumentRewriterTest);

HashIdDocumentRewriterTest::HashIdDocumentRewriterTest() {}

HashIdDocumentRewriterTest::~HashIdDocumentRewriterTest() {}

void HashIdDocumentRewriterTest::CaseSetUp() {}

void HashIdDocumentRewriterTest::CaseTearDown() {}

void HashIdDocumentRewriterTest::TestSimpleProcess()
{
    // test for add hash id field
    string fields = "string1:string;price:int32;";
    string indexes = "pk:primarykey64:string1";
    string attributes = "string1;price";
    auto schema = SchemaMaker::MakeSchema(fields, indexes, attributes, "");
    schema->SetEnableHashId(true);
    auto attrConfig = schema->GetAttributeSchema()->GetAttributeConfig(DEFAULT_HASH_ID_FIELD_NAME);

    HashIdDocumentRewriterPtr rewriter(new HashIdDocumentRewriter);
    ASSERT_NO_THROW(rewriter->Init(schema));

    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, "cmd=add,string1=hello,price=32");
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);
    size_t noEmptyFieldCount = attrDoc->GetNotEmptyFieldCount();

    // do nothing if hash id field tag not filled
    ASSERT_NO_THROW(rewriter->Rewrite(doc));
    ASSERT_EQ(noEmptyFieldCount, attrDoc->GetNotEmptyFieldCount());

    // add hash field to attr if hash id field tag filled
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "12");
    ASSERT_NO_THROW(rewriter->Rewrite(doc));
    ASSERT_EQ(noEmptyFieldCount + 1, attrDoc->GetNotEmptyFieldCount());
    autil::StringView field = attrDoc->GetField(attrConfig->GetFieldId());
    ASSERT_EQ((size_t)2, field.size());
    ASSERT_EQ((uint16_t)12, *((uint16_t*)field.data()));

    // repeated do rewrite
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "20");
    ASSERT_NO_THROW(rewriter->Rewrite(doc));
    field = attrDoc->GetField(attrConfig->GetFieldId());
    ASSERT_EQ((size_t)2, field.size());
    ASSERT_EQ((uint16_t)12, *((uint16_t*)field.data()));

    // test update field
    doc = DocumentCreator::CreateNormalDocument(schema, "cmd=update_field,string1=hello,price=32");
    attrDoc = doc->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);
    doc->AddTag(DOCUMENT_HASHID_TAG_KEY, "12");
    field = attrDoc->GetField(attrConfig->GetFieldId());
    ASSERT_EQ(autil::StringView::empty_instance(), field);

    ASSERT_NO_THROW(rewriter->Rewrite(doc));
    field = attrDoc->GetField(attrConfig->GetFieldId());
    ASSERT_EQ(autil::StringView::empty_instance(), field);
}
}} // namespace indexlib::document
