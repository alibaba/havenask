#include "indexlib/document/index_document/normal_document/test/attribute_document_field_extractor_unittest.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, AttributeDocumentFieldExtractorTest);

AttributeDocumentFieldExtractorTest::AttributeDocumentFieldExtractorTest() {}

AttributeDocumentFieldExtractorTest::~AttributeDocumentFieldExtractorTest() {}

void AttributeDocumentFieldExtractorTest::CaseSetUp() {}

void AttributeDocumentFieldExtractorTest::CaseTearDown() {}

void AttributeDocumentFieldExtractorTest::TestSimpleProcess()
{
    string fields = "string1:string;"
                    "attr1:int32;"
                    "attr2:int32:true:true;"
                    "attr3:string:true:true;"
                    "attr4:int32:true:true;"
                    "attr5:int32;attr6:string:false:true";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5,attr6";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fields, index, attribute, "");
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    auto packAttrConfig = attrSchema->GetPackAttributeConfig("packAttr2");
    packAttrConfig->EnableImpact();

    NormalDocumentPtr doc =
        DocumentCreator::CreateNormalDocument(schema, "cmd=add,string1=hello,attr1=11,attr2=12 20,"
                                                      "attr3=la la land,attr4=14 24,attr5=15,attr6=hello,"
                                                      "modify_fields=attr1#attr2#attr3#attr4#attr5#attr6");

    PackAttributeRewriter rewriter;
    ASSERT_TRUE(rewriter.Init(schema));
    rewriter.Rewrite(doc);

    AttributeDocumentFieldExtractorPtr extractor(new AttributeDocumentFieldExtractor());
    ASSERT_TRUE(extractor->Init(attrSchema));
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_TRUE(attrDoc);

    Pool* pool = doc->GetPool();
    StringView fieldValue = extractor->GetField(attrDoc, 0, pool);
    EXPECT_EQ(StringView::empty_instance(), fieldValue);

    CheckValue(extractor, attrSchema, attrDoc, 0, ft_int32, {});
    CheckValue(extractor, attrSchema, attrDoc, 1, ft_int32, {"11"});
    CheckValue(extractor, attrSchema, attrDoc, 2, ft_int32, {"12", "20"});
    CheckValue(extractor, attrSchema, attrDoc, 3, ft_string, {"la", "la", "land"});
    CheckValue(extractor, attrSchema, attrDoc, 4, ft_int32, {"14", "24"});
    CheckValue(extractor, attrSchema, attrDoc, 5, ft_int32, {"15"});
    CheckValue(extractor, attrSchema, attrDoc, 6, ft_string, {"hello"});
}

void AttributeDocumentFieldExtractorTest::TestExceptionCase()
{
    string fields = "string1:string;"
                    "attr1:int32;"
                    "attr2:int32:true:true;"
                    "attr3:string:true:true;"
                    "attr4:int32:true:true;"
                    "attr5:int32;"
                    "attr6:string:false:true";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5";
    string summary = "attr6";

    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(fields, index, attribute, summary);

    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();

    NormalDocumentPtr doc =
        DocumentCreator::CreateNormalDocument(schema, "cmd=add,string1=hello,attr1=11,attr4=14 24,attr5=15,attr6=hello,"
                                                      "modify_fields=attr1#attr2#attr3#attr4#attr5#attr6");

    PackAttributeRewriter rewriter;
    ASSERT_TRUE(rewriter.Init(schema));
    rewriter.Rewrite(doc);

    AttributeDocumentFieldExtractorPtr extractor(new AttributeDocumentFieldExtractor());
    AttributeSchemaPtr emptyAttrSchema;
    ASSERT_FALSE(extractor->Init(emptyAttrSchema));
    ASSERT_TRUE(extractor->Init(attrSchema));
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    AttributeDocumentPtr emptyAttrDoc;

    EXPECT_EQ(StringView::empty_instance(), extractor->GetField(emptyAttrDoc, 1, &mPool));
    EXPECT_TRUE(StringView::empty_instance() != extractor->GetField(attrDoc, 1, &mPool));
    attrDoc->ClearFields({1});
    EXPECT_EQ(StringView::empty_instance(), extractor->GetField(attrDoc, 1, &mPool));
    // get field which is no in AttributeSchema
    EXPECT_EQ(StringView::empty_instance(), extractor->GetField(attrDoc, 6, &mPool));
    // get field which is non-exist
    EXPECT_EQ(StringView::empty_instance(), extractor->GetField(attrDoc, 7, &mPool));
}

// expectValues.size() == 0, represents expect EMPTY_STRING
// FieldType support ft_int32 & ft_string only
void AttributeDocumentFieldExtractorTest::CheckValue(const AttributeDocumentFieldExtractorPtr& extractor,
                                                     const AttributeSchemaPtr& attrSchema,
                                                     const AttributeDocumentPtr& attrDoc, fieldid_t fieldId,
                                                     FieldType fieldType, const vector<string>& expectValues)
{
    if (expectValues.size() == 0) {
        EXPECT_EQ(StringView::empty_instance(), extractor->GetField(attrDoc, fieldId, &mPool));
        return;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
    FieldConfigPtr fieldConfig = attrConfig->GetFieldConfig();
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));

    StringView fieldValue = extractor->GetField(attrDoc, fieldId, &mPool);
    AttrValueMeta attrValueMeta = convertor->Decode(fieldValue);

    if (!fieldConfig->IsMultiValue()) {
        if (fieldType == ft_int32) {
            int32_t actualValue = *(int32_t*)attrValueMeta.data.data();
            int32_t expectValue = -1;
            StringUtil::strToInt32(expectValues[0].c_str(), expectValue);
            ASSERT_EQ(size_t(1), expectValues.size());
            EXPECT_EQ(actualValue, expectValue);
        } else {
            assert(fieldType == ft_string);
            MultiChar mc;
            mc.init(attrValueMeta.data.data());
            string actualValue(mc.data(), mc.size());
            EXPECT_EQ(expectValues[0], actualValue);
        }
    } else {
        if (fieldType == ft_int32) {
            MultiInt32 mv(attrValueMeta.data.data());
            for (size_t i = 0; i < expectValues.size(); ++i) {
                int32_t expectValue = -1;
                StringUtil::strToInt32(expectValues[i].c_str(), expectValue);
                EXPECT_EQ(expectValue, mv[i]);
            }
        } else {
            assert(fieldType == ft_string);
            MultiString mv(attrValueMeta.data.data());
            for (size_t i = 0; i < expectValues.size(); ++i) {
                MultiChar mc = mv[i];
                string actualValue(mc.data(), mc.size());
                EXPECT_EQ(expectValues[i], actualValue);
            }
        }
    }
}
}} // namespace indexlib::document
