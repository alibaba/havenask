#include "indexlib/partition/segment/test/sub_doc_segment_writer_unittest.h"

#include "indexlib/config/test/schema_maker.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/test/doc_id_manager_test_util.h"
#include "indexlib/partition/test/main_sub_test_util.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SubDocSegmentWriterTest);

class MockSubDocSegmentWriter : public SubDocSegmentWriter
{
public:
    MockSubDocSegmentWriter(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options)
        : SubDocSegmentWriter(schema, options)
    {
    }

public:
    virtual void UpdateMainDocJoinValue() {}

    void Init(const SingleSegmentWriterPtr& mainWriter, const SingleSegmentWriterPtr& subWriter)
    {
        mMainWriter = mainWriter;
        mSubWriter = subWriter;
        mMainModifier = mainWriter->GetInMemorySegmentModifier();
    }
};

SubDocSegmentWriterTest::SubDocSegmentWriterTest() {}

SubDocSegmentWriterTest::~SubDocSegmentWriterTest() {}

void SubDocSegmentWriterTest::CaseSetUp()
{
    _schema = partition::MainSubTestUtil::CreateSchema();
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);

    indexlib::file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    assert(rootDirectory);
    if (rootDirectory->IsExist(INDEX_FORMAT_VERSION_FILE_NAME)) {
        rootDirectory->RemoveFile(INDEX_FORMAT_VERSION_FILE_NAME);
    }
    indexlib::index_base::IndexFormatVersion indexFormatVersion(INDEX_FORMAT_VERSION);
    indexFormatVersion.Store(rootDirectory);
    rootDirectory->Sync(true);

    SchemaRewriter::Rewrite(_schema, IndexPartitionOptions(), GET_PARTITION_DIRECTORY());
}

void SubDocSegmentWriterTest::CaseTearDown() {}

void SubDocSegmentWriterTest::TestSimpleProcess()
{
    SubDocSegmentWriterPtr subDocSegmentWriter = MainSubTestUtil::PrepareSubSegmentWriter(_schema, GET_FILE_SYSTEM());

    SingleSegmentWriterPtr mainWriter = subDocSegmentWriter->GetMainWriter();
    SingleSegmentWriterPtr subWriter = subDocSegmentWriter->GetSubWriter();

    MockSubDocSegmentWriter mockSegmentWriter(_schema, IndexPartitionOptions());
    mockSegmentWriter.Init(mainWriter, subWriter);
    std::unique_ptr<MainSubDocIdManager> manager =
        DocIdManagerTestUtil::CreateMainSubDocIdManager(_schema, false, {}, 0, 0, GET_TEMP_DATA_PATH());

    string docString = "cmd=add,pkstr=1;";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(_schema, docString);
    ASSERT_TRUE(manager->Process(doc));
    INDEXLIB_TEST_TRUE(mockSegmentWriter.AddDocument(doc));

    InMemorySegmentReaderPtr mainReader = mainWriter->CreateInMemSegmentReader();
    InMemorySegmentReaderPtr subReader = subWriter->CreateInMemSegmentReader();

    AttributeSegmentReaderPtr mainAttrReader = mainReader->GetAttributeSegmentReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
    AttributeSegmentReaderPtr subAttrReader = subReader->GetAttributeSegmentReader(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
    docid_t mainJoin = INVALID_DOCID;
    bool isNull = false;

    auto ctx = mainAttrReader->CreateReadContextPtr(&_pool);
    mainAttrReader->Read(0, ctx, (uint8_t*)&mainJoin, 4, isNull);
    INDEXLIB_TEST_EQUAL(0, mainJoin);

    string docString2 = "cmd=add,pkstr=2,subpkstr=1^2";
    NormalDocumentPtr doc2 = DocumentCreator::CreateNormalDocument(_schema, docString2);
    ASSERT_TRUE(manager->Process(doc2));
    INDEXLIB_TEST_TRUE(mockSegmentWriter.AddDocument(doc2));

    // inconsistent, not update main join at this time
    ctx = mainAttrReader->CreateReadContextPtr(&_pool);
    mainAttrReader->Read(1, ctx, (uint8_t*)&mainJoin, 4, isNull);
    INDEXLIB_TEST_EQUAL(2, mainJoin);

    docid_t subJoin = INVALID_DOCID;
    ctx = subAttrReader->CreateReadContextPtr(&_pool);
    subAttrReader->Read(0, nullptr, (uint8_t*)&subJoin, 4, isNull);
    INDEXLIB_TEST_EQUAL(1, subJoin);

    ctx = subAttrReader->CreateReadContextPtr(&_pool);
    subAttrReader->Read(1, nullptr, (uint8_t*)&subJoin, 4, isNull);
    INDEXLIB_TEST_EQUAL(1, subJoin);
}

void SubDocSegmentWriterTest::TestEstimateInitMemUse()
{
    SegmentData segmentData;
    segmentData.SetSegmentId(0);
    SegmentDataPtr subSegmentData(new SegmentData);
    subSegmentData->SetSegmentId(0);
    segmentData.SetSubSegmentData(subSegmentData);

    SubDocSegmentWriter subDocWriter(_schema, IndexPartitionOptions());
    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);
    segmentMetrics->SetDistinctTermCount("pack1", 5 * 1024 * 1024);
    segmentMetrics->SetDistinctTermCount("subindex1", 5 * 1024 * 1024);
    size_t estimateMemUse =
        SubDocSegmentWriter::EstimateInitMemUse(segmentMetrics, _schema, IndexPartitionOptions(),
                                                plugin::PluginManagerPtr(), index_base::PartitionSegmentIteratorPtr());
    subDocWriter.Init(segmentData, segmentMetrics, util::CounterMapPtr(), plugin::PluginManagerPtr());
    string docString = "cmd=add,pkstr=2,text1=a,subpkstr=1^2,substr1=a^b";
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(_schema, docString);
    subDocWriter.AddDocument(doc);
    cout << "estimate mem use" << estimateMemUse << endl;
    cout << "actual mem use" << subDocWriter.GetTotalMemoryUse() << endl;
    size_t extraSize = autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE;
    ASSERT_TRUE(subDocWriter.GetTotalMemoryUse() < estimateMemUse + extraSize);
    ASSERT_TRUE(subDocWriter.GetTotalMemoryUse() > estimateMemUse);
}

void SubDocSegmentWriterTest::TestAddDocsFail()
{
    std::unique_ptr<MainSubDocIdManager> manager =
        DocIdManagerTestUtil::CreateMainSubDocIdManager(_schema, false, {}, 0, 0, GET_TEMP_DATA_PATH());

    {
        // check add main doc fail
        SubDocSegmentWriterPtr subDocSegmentWriter =
            MainSubTestUtil::PrepareSubSegmentWriter(_schema, GET_FILE_SYSTEM());
        string docString = "cmd=add,pkstr=2,subpkstr=1^2^1^3^4^3";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(_schema, docString);
        doc->SetIndexDocument(IndexDocumentPtr());
        ASSERT_FALSE(manager->Process(doc));
    }

    {
        // check add sub doc fail
        SubDocSegmentWriterPtr subDocSegmentWriter =
            MainSubTestUtil::PrepareSubSegmentWriter(_schema, GET_FILE_SYSTEM());
        string docString = "cmd=add,pkstr=2,subpkstr=1^2^3";
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(_schema, docString);
        NormalDocument::DocumentVector docVector = doc->GetSubDocuments();
        docVector[1]->SetIndexDocument(IndexDocumentPtr());
        ASSERT_TRUE(manager->Process(doc));
        ASSERT_TRUE(subDocSegmentWriter->AddDocument(doc));
        SingleSegmentWriterPtr mainWriter = subDocSegmentWriter->GetMainWriter();
        SingleSegmentWriterPtr subWriter = subDocSegmentWriter->GetSubWriter();
        InMemorySegmentReaderPtr mainReader = mainWriter->CreateInMemSegmentReader();
        InMemorySegmentReaderPtr subReader = subWriter->CreateInMemSegmentReader();

        AttributeSegmentReaderPtr mainAttrReader =
            mainReader->GetAttributeSegmentReader(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME);
        AttributeSegmentReaderPtr subAttrReader =
            subReader->GetAttributeSegmentReader(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME);
        docid_t mainJoin = INVALID_DOCID;
        bool isNull = false;
        auto ctx = mainAttrReader->CreateReadContextPtr(&_pool);
        mainAttrReader->Read(0, ctx, (uint8_t*)&mainJoin, 4, isNull);
        ASSERT_EQ((docid_t)2, mainJoin);
    }
}

void SubDocSegmentWriterTest::TestAllocateBuildResourceMetricsNode()
{
    IndexPartitionSchemaPtr mainSchema =
        SchemaMaker::MakeSchema("string1:string;string2:string", "pk:primarykey64:string1", "string2;", "");

    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_str1:string;sub_str2:string;", "sub_pk:primarykey64:sub_str1", "sub_str2", "");
    mainSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::Rewrite(mainSchema, IndexPartitionOptions(), GET_PARTITION_DIRECTORY());

    SegmentDataPtr segmentData(new SegmentData);
    SegmentDataPtr subSegmentData(new SegmentData);
    segmentData->SetSubSegmentData(subSegmentData);

    SubDocSegmentWriter segWriter(mainSchema, IndexPartitionOptions());
    std::shared_ptr<indexlib::framework::SegmentMetrics> segMetrics(new framework::SegmentMetrics);
    util::CounterMapPtr counterMap;
    plugin::PluginManagerPtr pluginManager;
    segWriter.Init(*segmentData, segMetrics, counterMap, pluginManager);

    ASSERT_TRUE(segWriter.mMainWriter);
    ASSERT_TRUE(segWriter.mSubWriter);

    const util::BuildResourceMetricsPtr& buildMetrics = segWriter.GetBuildResourceMetrics();

    ASSERT_TRUE(buildMetrics);
    // main pk , main pk attribute, string2, main deletion map,
    // sub pk , sub pk attribute, sub_str2, sub deletion map,
    // main_docid_to_sub_docid, sub_docid_to_main_docid
    ASSERT_EQ(10, buildMetrics->GetNodeCount());
}

}} // namespace indexlib::partition
