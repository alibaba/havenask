#include "indexlib/index_base/segment/test/in_memory_segment_unittest.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, InMemorySegmentTest);

InMemorySegmentTest::InMemorySegmentTest()
{
}

InMemorySegmentTest::~InMemorySegmentTest()
{
}

void InMemorySegmentTest::CaseSetUp()
{
    mSchema = SchemaMaker::MakeSchema("str:string;", "idx:primarykey64:str;", "", "");
    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_str:string;", "sub_idx:primarykey64:sub_str;", "", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::Rewrite(mSchema, IndexPartitionOptions(),
                            GET_TEST_DATA_PATH());
}

void InMemorySegmentTest::CaseTearDown()
{
}

void InMemorySegmentTest::TestDump()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    InMemorySegment inMemSegment(
            options.GetBuildConfig(),
            util::MemoryQuotaControllerCreator::CreateBlockMemoryController(),
            util::CounterMapPtr());
    
    BuildingSegmentData segData(options.GetBuildConfig());
    segData.Init(partDirectory);
    segmentid_t segId = 1;
    segData.SetSegmentId(segId);
    Version versin;
    string segName = versin.GetNewSegmentDirName(segId);
    segData.SetSegmentDirName(segName);
    inMemSegment.Init(segData, false);
    ASSERT_FALSE(partDirectory->IsExist(segName));
    inMemSegment.BeginDump();

    DirectoryPtr segDirectory = inMemSegment.GetDirectory();
    ASSERT_FALSE(segDirectory->IsExist(SEGMENT_INFO_FILE_NAME));
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(InMemDirectory, segDirectory));
    inMemSegment.EndDump();
    ASSERT_TRUE(segDirectory->IsExist(SEGMENT_INFO_FILE_NAME));
    ASSERT_TRUE(segDirectory->IsExist(SEGMENT_FILE_LIST));
}

void InMemorySegmentTest::TestDumpForSub()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();

    IndexPartitionOptions options;
    options.SetEnablePackageFile(false);
    BuildingSegmentData segmentData(options.GetBuildConfig());
    segmentData.SetSegmentId(1);
    segmentData.SetBaseDocId(0);
    Version version;
    string segName = version.GetNewSegmentDirName(segmentData.GetSegmentId());
    segmentData.SetSegmentDirName(segName);
    SegmentDataPtr subSegmentData(new BuildingSegmentData(segmentData));
    segmentData.SetSubSegmentData(subSegmentData);

    InMemorySegmentCreator creator;
    creator.Init(mSchema, options);
    util::CounterMapPtr counterMap(new util::CounterMap);
    plugin::PluginManagerPtr nullPluginManager;
    InMemorySegmentPtr inMemSegment = 
        creator.Create(PartitionSegmentIteratorPtr(),
                       segmentData, partDirectory,
                       util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(),
                       counterMap, nullPluginManager);

    string docString = "cmd=add,str=hello,sub_str=hi;";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);

    inMemSegment->GetSegmentWriter()->AddDocument(doc);

    ASSERT_FALSE(partDirectory->IsExist(segName));
    inMemSegment->BeginDump();
    ASSERT_TRUE(partDirectory->IsExist(segName));

    DirectoryPtr segDirectory = inMemSegment->GetDirectory();
    ASSERT_TRUE(segDirectory->IsExist(SUB_SEGMENT_DIR_NAME));
    ASSERT_FALSE(segDirectory->IsExist(SEGMENT_INFO_FILE_NAME));

    DirectoryPtr indexDirectory = segDirectory->GetDirectory(INDEX_DIR_NAME, true);
    ASSERT_TRUE(indexDirectory->IsExist("idx/data"));

    DirectoryPtr subSegDirectory = segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, true);
    ASSERT_FALSE(subSegDirectory->IsExist(SEGMENT_INFO_FILE_NAME));

    DirectoryPtr subIndexDirectory = subSegDirectory->GetDirectory(INDEX_DIR_NAME, true);
    ASSERT_TRUE(subIndexDirectory->IsExist("sub_idx/data"));

    ASSERT_TRUE(DYNAMIC_POINTER_CAST(InMemDirectory, segDirectory));
    inMemSegment->EndDump();
    ASSERT_TRUE(segDirectory->IsExist(SEGMENT_INFO_FILE_NAME));
    ASSERT_TRUE(subSegDirectory->IsExist(SEGMENT_INFO_FILE_NAME));

    ASSERT_TRUE(segDirectory->IsExist(SEGMENT_FILE_LIST));    

    segDirectory->Sync(true);
}

IE_NAMESPACE_END(index_base);

