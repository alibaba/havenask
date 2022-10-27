#include "indexlib/partition/segment/test/sub_doc_segment_writer_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/in_memory_segment_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, SubDocSegmentWriterTest);

class MockSubDocSegmentWriter : public SubDocSegmentWriter
{
public:
    MockSubDocSegmentWriter(
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options)
        : SubDocSegmentWriter(schema, options)
    {}

public:
    virtual void UpdateMainDocJoinValue()
    {}

    void Init(const SingleSegmentWriterPtr& mainWriter,
              const SingleSegmentWriterPtr& subWriter)
    {
        mMainWriter = mainWriter;
        mSubWriter = subWriter;
        mMainModifier = mainWriter->GetInMemorySegmentModifier();
        InitFieldIds(mSchema);
    }
};

SubDocSegmentWriterTest::SubDocSegmentWriterTest()
{
}

SubDocSegmentWriterTest::~SubDocSegmentWriterTest()
{
}

void SubDocSegmentWriterTest::CaseSetUp()
{
    string field = "pkstr:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;";
    string summary = "pkstr;";

    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
                            "substr1:string;subpkstr:string;sub_long:uint32;",
                            "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                            "substr1;subpkstr;sub_long;",
                            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mRootDir = GET_TEST_DATA_PATH();

    SchemaRewriter::Rewrite(mSchema, mOptions, mRootDir);
}

void SubDocSegmentWriterTest::CaseTearDown()
{
}

void SubDocSegmentWriterTest::TestSimpleProcess()
{
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), mOptions, mSchema);
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    INDEXLIB_TEST_TRUE(inMemSegment);

    SubDocSegmentWriterPtr subDocSegmentWriter = DYNAMIC_POINTER_CAST(
            SubDocSegmentWriter, inMemSegment->GetSegmentWriter());
    INDEXLIB_TEST_TRUE(subDocSegmentWriter);

    SingleSegmentWriterPtr mainWriter = subDocSegmentWriter->GetMainWriter();
    SingleSegmentWriterPtr subWriter = subDocSegmentWriter->GetSubWriter();

    MockSubDocSegmentWriter mockSegmentWriter(mSchema, mOptions);
    mockSegmentWriter.Init(mainWriter, subWriter);

    string docString = "cmd=add,pkstr=1;";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    INDEXLIB_TEST_TRUE(mockSegmentWriter.AddDocument(doc));

    InMemorySegmentReaderPtr mainReader = mainWriter->CreateInMemSegmentReader();
    InMemorySegmentReaderPtr subReader = subWriter->CreateInMemSegmentReader();

    AttributeSegmentReaderPtr mainAttrReader = mainReader->GetAttributeSegmentReader(
            MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeSegmentReaderPtr subAttrReader = subReader->GetAttributeSegmentReader(
            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    docid_t mainJoin = INVALID_DOCID;
    mainAttrReader->Read(0, (uint8_t*)&mainJoin, 4);
    INDEXLIB_TEST_EQUAL(INVALID_DOCID, mainJoin);

    string docString2 = "cmd=add,pkstr=2,subpkstr=1^2";
    NormalDocumentPtr doc2 = DocumentCreator::CreateDocument(mSchema, docString2);
    INDEXLIB_TEST_TRUE(mockSegmentWriter.AddDocument(doc2));
    
    //inconsistent, not update main join at this time
    mainAttrReader->Read(1, (uint8_t*)&mainJoin, 4);
    INDEXLIB_TEST_EQUAL(INVALID_DOCID, mainJoin);

    docid_t subJoin = INVALID_DOCID;
    subAttrReader->Read(0, (uint8_t*)&subJoin, 4);
    INDEXLIB_TEST_EQUAL(1, subJoin);

    subAttrReader->Read(1, (uint8_t*)&subJoin, 4);
    INDEXLIB_TEST_EQUAL(1, subJoin);
}

void SubDocSegmentWriterTest::TestEstimateInitMemUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    SegmentDataPtr subSegmentData(new SegmentData);
    subSegmentData->SetSegmentId(0);
    segmentData.SetSubSegmentData(subSegmentData);
    
    SubDocSegmentWriter subDocWriter(mSchema, mOptions);
    SegmentMetricsPtr segmentMetrics(new SegmentMetrics);
    segmentMetrics->SetDistinctTermCount("pack1", 5 * 1024 * 1024);
    segmentMetrics->SetDistinctTermCount("subindex1", 5 * 1024 * 1024);
    size_t estimateMemUse = subDocWriter.EstimateInitMemUse(
            segmentMetrics, util::QuotaControlPtr());
    subDocWriter.Init(segmentData, segmentMetrics, util::CounterMapPtr(), plugin::PluginManagerPtr());
    string docString = "cmd=add,pkstr=2,text1=a,subpkstr=1^2,substr1=a^b";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    subDocWriter.AddDocument(doc);
    cout << "estimate mem use" << estimateMemUse << endl;
    cout << "actual mem use" << subDocWriter.GetTotalMemoryUse() << endl;
    size_t extraSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    ASSERT_TRUE(subDocWriter.GetTotalMemoryUse() < estimateMemUse + extraSize);
    ASSERT_TRUE(subDocWriter.GetTotalMemoryUse() > estimateMemUse);
}

void SubDocSegmentWriterTest::TestAddDocsFail()
{
    {
        //check add main doc fail
        SubDocSegmentWriterPtr subDocSegmentWriter = PrepareSubSegmentWriter();
        string docString = "cmd=add,pkstr=2,subpkstr=1^2^1^3^4^3";
        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
        doc->SetIndexDocument(IndexDocumentPtr());
        ASSERT_FALSE(subDocSegmentWriter->AddDocument(doc));
    }

    {
        //check add sub doc fail
        SubDocSegmentWriterPtr subDocSegmentWriter = PrepareSubSegmentWriter();
        string docString = "cmd=add,pkstr=2,subpkstr=1^2^3";
        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
        NormalDocument::DocumentVector docVector = doc->GetSubDocuments();
        docVector[1]->SetIndexDocument(IndexDocumentPtr());
        ASSERT_TRUE(subDocSegmentWriter->AddDocument(doc));
        SingleSegmentWriterPtr mainWriter = subDocSegmentWriter->GetMainWriter();
        SingleSegmentWriterPtr subWriter = subDocSegmentWriter->GetSubWriter();
        InMemorySegmentReaderPtr mainReader = mainWriter->CreateInMemSegmentReader();
        InMemorySegmentReaderPtr subReader = subWriter->CreateInMemSegmentReader();
        
        AttributeSegmentReaderPtr mainAttrReader = mainReader->GetAttributeSegmentReader(
                MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
        AttributeSegmentReaderPtr subAttrReader = subReader->GetAttributeSegmentReader(
                SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
        docid_t mainJoin = INVALID_DOCID;
        mainAttrReader->Read(0, (uint8_t*)&mainJoin, 4);
        ASSERT_EQ((docid_t)2, mainJoin);
    }
}

void SubDocSegmentWriterTest::TestDedupSubDoc()
{
    SubDocSegmentWriterPtr subDocSegmentWriter = PrepareSubSegmentWriter();
    string docString = "cmd=add,pkstr=2,subpkstr=1^2^1^3^4^3";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    subDocSegmentWriter->DedupSubDoc(doc);
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    ASSERT_EQ((size_t)4, subDocs.size());
    ASSERT_EQ(string("2"), subDocs[0]->GetPrimaryKey());
    ASSERT_EQ(string("1"), subDocs[1]->GetPrimaryKey());
    ASSERT_EQ(string("4"), subDocs[2]->GetPrimaryKey());
    ASSERT_EQ(string("3"), subDocs[3]->GetPrimaryKey());
}

void SubDocSegmentWriterTest::TestAddJoinFieldToDocument()
{
    {
        //check doc without attribute
        SubDocSegmentWriterPtr subDocSegmentWriter = PrepareSubSegmentWriter();
        string docString = "cmd=add,pkstr=2,subpkstr=1^2";
        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
        RemoveAttributeDocument(doc);
        subDocSegmentWriter->AddJoinFieldToDocument(doc);
        CheckJoinFieldInDoc(doc, 0);
        subDocSegmentWriter->AddDocument(doc);
        RemoveAttributeDocument(doc);
        subDocSegmentWriter->AddJoinFieldToDocument(doc);
        CheckJoinFieldInDoc(doc, 1);
    }

    {
        //check doc has attribute
        SubDocSegmentWriterPtr subDocSegmentWriter = PrepareSubSegmentWriter();
        string docString = "cmd=add,pkstr=2,long1=3,subpkstr=1^2,sub_long=4^5";
        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
        subDocSegmentWriter->AddJoinFieldToDocument(doc);
        CheckJoinFieldInDoc(doc, 0);
        
        subDocSegmentWriter->AddDocument(doc);
        subDocSegmentWriter->AddJoinFieldToDocument(doc);
        CheckJoinFieldInDoc(doc, 1);
    }
}

void SubDocSegmentWriterTest::TestAllocateBuildResourceMetricsNode()
{
    IndexPartitionSchemaPtr mainSchema =
        SchemaMaker::MakeSchema("string1:string;string2:string", "pk:primarykey64:string1",
                                "string2;", "");

    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_str1:string;sub_str2:string;", "sub_pk:primarykey64:sub_str1",
                                "sub_str2", "");
    mainSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::Rewrite(mainSchema, mOptions, GET_TEST_DATA_PATH());

    SegmentDataPtr segmentData(new SegmentData);
    SegmentDataPtr subSegmentData(new SegmentData);
    segmentData->SetSubSegmentData(subSegmentData);
    
    SubDocSegmentWriter segWriter(mainSchema, mOptions);
    SegmentMetricsPtr segMetrics(new SegmentMetrics);
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    segWriter.Init(*segmentData, segMetrics, counterMap, pluginManager);

    ASSERT_TRUE(segWriter.mMainWriter);
    ASSERT_TRUE(segWriter.mSubWriter);

    const util::BuildResourceMetricsPtr& buildMetrics =
        segWriter.GetBuildResourceMetrics();

    ASSERT_TRUE(buildMetrics);
    // main pk , main pk attribute, string2, main deletion map,
    // sub pk , sub pk attribute, sub_str2, sub deletion map,
    // main_docid_to_sub_docid, sub_docid_to_main_docid
    ASSERT_EQ(10, buildMetrics->GetNodeCount());
}

void SubDocSegmentWriterTest::RemoveAttributeDocument(NormalDocumentPtr& doc)
{
    doc->SetAttributeDocument(AttributeDocumentPtr());
    NormalDocument::DocumentVector subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); i++)
    {
        subDocs[i]->SetAttributeDocument(AttributeDocumentPtr());
    }
}

void SubDocSegmentWriterTest::CheckJoinFieldInDoc(
        const NormalDocumentPtr& doc, docid_t subJoinValue)
{
    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    fieldid_t mainFieldId = mSchema->GetFieldSchema()->GetFieldId(
            MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    fieldid_t subFieldId = subSchema->GetFieldSchema()->GetFieldId(
            SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);

    ConstString fieldValue = doc->GetAttributeDocument()->GetField(mainFieldId);
    ASSERT_EQ(INVALID_DOCID, *(docid_t*)fieldValue.data());

    NormalDocument::DocumentVector subDocs = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocs.size(); i++)
    {
        ConstString subFieldValue = 
            subDocs[i]->GetAttributeDocument()->GetField(subFieldId);
        ASSERT_EQ(subJoinValue, *(docid_t*)subFieldValue.data());
    }
}

SubDocSegmentWriterPtr SubDocSegmentWriterTest::PrepareSubSegmentWriter()
{
    TearDown();
    SetUp();
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(
            GET_FILE_SYSTEM(), mOptions, mSchema);
    mInMemSeg = partitionData->CreateNewSegment();
    SubDocSegmentWriterPtr subDocSegmentWriter = DYNAMIC_POINTER_CAST(
            SubDocSegmentWriter, mInMemSeg->GetSegmentWriter());
    return subDocSegmentWriter;
}

IE_NAMESPACE_END(partition);

