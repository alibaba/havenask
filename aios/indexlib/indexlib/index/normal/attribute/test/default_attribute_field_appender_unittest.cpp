#include "indexlib/index/normal/attribute/test/default_attribute_field_appender_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefaultAttributeFieldAppenderTest);

DefaultAttributeFieldAppenderTest::DefaultAttributeFieldAppenderTest()
{
}

DefaultAttributeFieldAppenderTest::~DefaultAttributeFieldAppenderTest()
{
}

void DefaultAttributeFieldAppenderTest::CaseSetUp()
{
    string field = "string1:string;title:string;price:uint32;count:int64";
    string index = "pk:primarykey64:string1";
    string attribute = "title;price;count";

    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    FieldConfigPtr virtualField(new FieldConfig(
                    "virtual_attribute", ft_int16, false));
    virtualField->SetDefaultValue("100");
    AttributeConfigPtr attrConfig(new AttributeConfig(
                    AttributeConfig::ct_virtual));
    attrConfig->Init(virtualField);
    mSchema->AddVirtualAttributeConfig(attrConfig);
}

void DefaultAttributeFieldAppenderTest::CaseTearDown()
{
}

void DefaultAttributeFieldAppenderTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");

    DefaultAttributeFieldAppender assembler;
    assembler.Init(mSchema, InMemorySegmentReaderPtr());

    NormalDocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(ADD_DOC);
    AttributeDocumentPtr attrDoc(new AttributeDocument);
    doc->SetAttributeDocument(attrDoc);

    assembler.AppendDefaultFieldValues(doc);
    ASSERT_EQ((uint32_t)4, attrDoc->GetNotEmptyFieldCount());

    AttributeSchemaPtr virAttrSchema = mSchema->GetVirtualAttributeSchema();
    fieldid_t fieldId = 
        virAttrSchema->GetAttributeConfig("virtual_attribute")->GetFieldId();

    const ConstString& fieldValue = 
        doc->GetAttributeDocument()->GetField(fieldId);
    SingleValueAttributeConvertor<int16_t> convertor;
    AttrValueMeta meta = convertor.Decode(fieldValue);
    ASSERT_EQ((int16_t)100, *(int16_t*)meta.data.data());
}

IE_NAMESPACE_END(index);

