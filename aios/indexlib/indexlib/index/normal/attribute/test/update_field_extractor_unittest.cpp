#include "indexlib/index/normal/attribute/test/update_field_extractor_unittest.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UpdateFieldExtractorTest);

UpdateFieldExtractorTest::UpdateFieldExtractorTest()
{
}

UpdateFieldExtractorTest::~UpdateFieldExtractorTest()
{
}

void UpdateFieldExtractorTest::CaseSetUp()
{
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            "price:int32;sale:int32;nid:int64", // this is field schema
            "nid:primarykey64:nid", // this is index schema
            "price;sale;nid", // this is attribute schema
            "" );// this is summary schema

    mDocStr = "{ 0,"
              "[price: (3)],"                                     \
              "[nid: (1)],"                                       \
              "[sale: (10)]"                                      \
              "}";

    DocumentMaker::MockIndexPart answer;
    mDoc = DYNAMIC_POINTER_CAST(NormalDocument,
                                DocumentMaker::MakeDocument(0, mDocStr, mSchema, answer));
}

void UpdateFieldExtractorTest::CaseTearDown()
{
}

//expectFieldInfos = "fieldid,value;fieldid,value;..."
void UpdateFieldExtractorTest::InnerTestInit(
        const IndexPartitionSchemaPtr& schema, 
        const AttributeDocumentPtr& attrDoc,
        const string& expectFieldInfos)
{
    vector<vector<int32_t> > fieldInfos;
    StringUtil::fromString(expectFieldInfos, fieldInfos, ",", ";");

    UpdateFieldExtractor extractor(schema);
    extractor.Init(attrDoc);
    ASSERT_EQ(fieldInfos.size(), extractor.GetFieldCount());

    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    for (size_t i = 0; i < fieldInfos.size(); i++)
    {
        ASSERT_TRUE(iter.HasNext());
        fieldid_t fieldId = 0;
        ConstString value = iter.Next(fieldId);
        assert(fieldInfos[i].size() == 2);
        ASSERT_EQ((fieldid_t)fieldInfos[i][0], fieldId);
        ASSERT_EQ(fieldInfos[i][1], *(int32_t*)value.data());
    }

    ASSERT_FALSE(iter.HasNext());
}

void UpdateFieldExtractorTest::TestInit()
{
    {
        //test empty attribute document
        InnerTestInit(mSchema, AttributeDocumentPtr(), "");
    }

    {
        // no attribute schema
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema,
                "nid:int64", "nid:primarykey64:nid", "", "");
        InnerTestInit(schema, mDoc->GetAttributeDocument(), "");
    }

    {
        // attribute field empty
        DocumentMaker::MockIndexPart answer;
        NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(
                NormalDocument, DocumentMaker::MakeDocument(0, mDocStr, mSchema, answer));
        ConstString emptyValue;
        AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
        attrDoc->SetField(1, emptyValue);
        
        InnerTestInit(mSchema, attrDoc, "0,3");
    }

    {
        // not exist field
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, 
                "sale:int32;nid:int64", 
                "nid:primarykey64:nid", "sale", "");
        InnerTestInit(schema, mDoc->GetAttributeDocument(), "");
    }

    {
        // not exist attribute field
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, 
                "price:int32;sale:int32;nid:int64", 
                "nid:primarykey64:nid", "sale", "");
        InnerTestInit(schema, mDoc->GetAttributeDocument(), "1,10");
    }


    {
        // not updatable attribute
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, 
                "price:string;sale:int32;nid:int64", 
                "nid:primarykey64:nid", "price;sale", "");
        InnerTestInit(schema, mDoc->GetAttributeDocument(), "1,10");
    }

    {
        // updatable attribute also in index
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(schema, 
                "price:int32;sale:int32;nid:int64", 
                "nid:primarykey64:nid;sale:number:sale", "price;sale;nid", "");
        InnerTestInit(schema, mDoc->GetAttributeDocument(), "0,3;1,10");
    }
}

IE_NAMESPACE_END(index);

