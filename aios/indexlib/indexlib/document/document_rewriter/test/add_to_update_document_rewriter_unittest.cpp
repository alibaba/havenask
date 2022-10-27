#include "indexlib/document/document_rewriter/test/add_to_update_document_rewriter_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, AddToUpdateDocumentRewriterTest);

AddToUpdateDocumentRewriterTest::AddToUpdateDocumentRewriterTest()
{
}

AddToUpdateDocumentRewriterTest::~AddToUpdateDocumentRewriterTest()
{
}

void AddToUpdateDocumentRewriterTest::CaseSetUp()
{
}

void AddToUpdateDocumentRewriterTest::CaseTearDown()
{
}

void AddToUpdateDocumentRewriterTest::TestFilterTruncateSortFields()
{
    string fields = "string1:string;price:int32;"
                    "biz30day:int32;nid:int32;desc_biz30day:int32;"
                    "sales:int32;end1:int32;end:int16";
    string indexes = "pk:primarykey64:string1";
    string attributes = "string1;price;biz30day;nid;"
                        "desc_biz30day;sales;end1;end";
    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(fields, indexes, attributes, "");
    AddToUpdateDocumentRewriter reWriter;
    reWriter.mFieldSchema = schema->GetFieldSchema();
    reWriter.AllocFieldBitmap();

    vector<SortDescriptions> partitionMetaVec(1);
    reWriter.AddUpdatableFields(schema, partitionMetaVec);
    ASSERT_EQ((uint32_t)8, reWriter.mUpdatableFieldIds.GetItemCount());
    ASSERT_EQ((uint32_t)7, reWriter.mUpdatableFieldIds.GetSetCount());
    ASSERT_FALSE(reWriter.mUpdatableFieldIds.Test(0));

    //test empty truncate options
    TruncateOptionConfigPtr truncOptConfig(new TruncateOptionConfig);
    reWriter.FilterTruncateSortFields(truncOptConfig);
    ASSERT_EQ((uint32_t)7, reWriter.mUpdatableFieldIds.GetSetCount());
    ASSERT_FALSE(reWriter.mUpdatableFieldIds.Test(0));    
    
    //test filter by truncate options
    string fileName = string(TEST_DATA_PATH) 
                      + "truncate_config_option_"
                      "for_index_partition_writer.json";
    
    string jsonString;
    FileSystemWrapper::AtomicLoad(fileName, jsonString);
    truncOptConfig = TruncateConfigMaker::MakeConfigFromJson(jsonString);
    ASSERT_TRUE(truncOptConfig);

    IndexPartitionSchemaPtr schema_with_truncate =
	SchemaMaker::MakeSchema(
            // fields
	    "string1:string;price:int32;"
	    "biz30day:int32;nid:int32;desc_biz30day:int32;"
	    "sales:int32;end1:int32;end:int16",
	    // indexes
	    "pk:primarykey64:string1:true;",
	    // attributes
	    "string1;price;biz30day;nid;"
	    "desc_biz30day;sales;end1;end",
	    // summary
	    "",
	    // truncate string
	    "desc_price:-price#desc_biz30day:-biz30day;+nid#desc_sales:-sales#galaxy_weight"
	    );
    
    truncOptConfig->Init(schema_with_truncate);
    reWriter.FilterTruncateSortFields(truncOptConfig);
    ASSERT_EQ((uint32_t)2, reWriter.mUpdatableFieldIds.GetSetCount());
    ASSERT_TRUE(reWriter.mUpdatableFieldIds.Test(6));    
    ASSERT_TRUE(reWriter.mUpdatableFieldIds.Test(7));
}

void AddToUpdateDocumentRewriterTest::TestAddUpdatableFields()
{
    string fields = "string1:string;sortField:int32;"
                    "attr1:string:true:true;attrInIndex:int32;"
                    "attrNotUpdatable:string;summaryField:int64;"
                    "attrInSummary:int32;uselessField:int32;";
    string indexes = "stringI:string:string1;"
                     "pk:primarykey64:string1;"
                     "attrInIndex1:number:attrInIndex";
    string attributes = "string1;attrInIndex;"
                        "attr1;attrNotUpdatable;"
                        "attrInSummary;sortField";
    string summarys = "string1;attrInSummary;summaryField";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            fields, indexes, attributes, summarys);
    
    vector<SortDescriptions> partitionMetaVec(1);
    partitionMetaVec[0].push_back(SortDescription("sortField", sp_asc));

    AddToUpdateDocumentRewriter reWriter;
    reWriter.mFieldSchema = schema->GetFieldSchema();
    reWriter.AllocFieldBitmap();
    reWriter.AddUpdatableFields(schema, partitionMetaVec);

    ASSERT_EQ((uint32_t)8, reWriter.mUpdatableFieldIds.GetItemCount());
    ASSERT_EQ((uint32_t)2, reWriter.mUpdatableFieldIds.GetSetCount());
    ASSERT_TRUE(reWriter.mUpdatableFieldIds.Test(2));
    ASSERT_TRUE(reWriter.mUpdatableFieldIds.Test(6));

    ASSERT_EQ((uint32_t)8, reWriter.mUselessFieldIds.GetItemCount());
    ASSERT_EQ((uint32_t)1, reWriter.mUselessFieldIds.GetSetCount());
    ASSERT_TRUE(reWriter.mUselessFieldIds.Test(7));
}

void AddToUpdateDocumentRewriterTest::TestRewriteForPackFields()
{
    string fields = "string1:string;attr1:int32;attr2:int32;attr3:int32;attr4:int8;attr5:int16";
    string index = "pk:primarykey64:string1";
    string attribute = "attr1;packAttr1:attr2,attr3;packAttr2:attr4,attr5";
    
    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(fields, index, attribute, "");

    vector<TruncateOptionConfigPtr> truncateOptionConfigs(1);
    vector<SortDescriptions> partMetaVec(1);

    //test rewrite success
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(
            schema, "cmd=add,string1=hello,attr1=11,attr2=12,attr3=13,attr4=14,attr5=15,"
            "modify_fields=attr1#attr2#attr3#attr4#attr5"); 

    PackAttributeRewriter packRewriter;
    packRewriter.Init(schema);
    packRewriter.Rewrite(doc);

    AddToUpdateDocumentRewriter reWriter;
    reWriter.Init(schema, truncateOptionConfigs, partMetaVec);

    reWriter.Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)5, attrDoc->GetNotEmptyFieldCount());
    CheckDocumentModifiedFields(doc);

    for (fieldid_t fid = 1; fid <= (fieldid_t)5; ++fid)
    {
        ASSERT_TRUE(attrDoc->HasField(fid));
    }
}

void AddToUpdateDocumentRewriterTest::TestRewrite()
{
    string fields = "string1:string;price:int32;useless:int32";
    string indexes = "pk:primarykey64:string1";
    string attributes = "string1;price";
    IndexPartitionSchemaPtr schema = 
        SchemaMaker::MakeSchema(fields, indexes, attributes, "");

    vector<TruncateOptionConfigPtr> truncateOptionConfigs(1);
    vector<SortDescriptions> partMetaVec(1);

    AddToUpdateDocumentRewriter reWriter;
    reWriter.Init(schema, truncateOptionConfigs, partMetaVec);

    //test rewrite fail
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(
            schema, "cmd=add,string1=hello,price=32,"
            "modify_fields=string1#price");
    reWriter.Rewrite(doc);
    ASSERT_EQ(ADD_DOC, doc->GetDocOperateType());
    CheckDocumentModifiedFields(doc);

    doc = DocumentCreator::CreateDocument(
            schema, "cmd=delete,string1=hello,price=32,"
            "modify_fields=string1#price");
    reWriter.Rewrite(doc);
    ASSERT_EQ(DELETE_DOC, doc->GetDocOperateType());

    //test rewrite success
    doc = DocumentCreator::CreateDocument(
            schema, "cmd=add,string1=hello,price=32,modify_fields=price");
    reWriter.Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, attrDoc->GetNotEmptyFieldCount());
    ASSERT_TRUE(attrDoc->HasField(1));

    CheckDocumentModifiedFields(doc);

    //test rewrite success, modify_fields has useless field
    doc = DocumentCreator::CreateDocument(
            schema, "cmd=add,string1=hello,price=32,useless=30,"
            "modify_fields=price#useless");
    reWriter.Rewrite(doc);
    ASSERT_EQ(UPDATE_FIELD, doc->GetDocOperateType());
    ASSERT_EQ(ADD_DOC, doc->GetOriginalOperateType());

    attrDoc = doc->GetAttributeDocument();
    ASSERT_EQ((uint32_t)1, attrDoc->GetNotEmptyFieldCount());
    CheckDocumentModifiedFields(doc);
}

void AddToUpdateDocumentRewriterTest::CheckDocumentModifiedFields(
        const NormalDocumentPtr& doc)
{
    const NormalDocument::FieldIdVector& modifiedFields = doc->GetModifiedFields();
    const NormalDocument::FieldIdVector& subModifiedFields = 
        doc->GetSubModifiedFields();

    ASSERT_TRUE(modifiedFields.empty());
    ASSERT_TRUE(subModifiedFields.empty());

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++)
    {
        CheckDocumentModifiedFields(subDocuments[i]);
    }
}

IE_NAMESPACE_END(document);

