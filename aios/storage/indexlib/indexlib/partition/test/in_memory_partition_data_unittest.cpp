#include "indexlib/partition/test/in_memory_partition_data_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/segment/test/segment_iterator_helper.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemoryPartitionDataTest);

namespace {
class MockSegmentDumpItem : public NormalSegmentDumpItem
{
public:
    MockSegmentDumpItem(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                        const InMemorySegmentPtr& inMemSeg)
        : NormalSegmentDumpItem(options, schema, "")
    {
        mInMemorySegment = inMemSeg;
    }
    ~MockSegmentDumpItem() {}
};
} // namespace

InMemoryPartitionDataTest::InMemoryPartitionDataTest() {}

InMemoryPartitionDataTest::~InMemoryPartitionDataTest() {}

void InMemoryPartitionDataTest::CaseSetUp()
{
    string field = "pkstr:string;text1:text;long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pkstr;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "pkstr;";

    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);

    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema("substr1:string;subpkstr:string;sub_long:uint32;",
                                                                "subindex1:string:substr1;sub_pk:primarykey64:subpkstr",
                                                                "substr1;subpkstr;sub_long;", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    SchemaRewriter::RewriteForSubTable(mSchema);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void InMemoryPartitionDataTest::CaseTearDown() {}

void InMemoryPartitionDataTest::TestAddSegmentForSub()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 10, true);
    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSIONID), true);

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    InMemoryPartitionData partitionData(dumpContainer);
    partitionData.Open(segDir);

    PartitionDataPtr subPartitionData = partitionData.GetSubPartitionData();
    INDEXLIB_TEST_TRUE(subPartitionData);
    INDEXLIB_TEST_EQUAL(subPartitionData->GetVersion(), partitionData.GetVersion());

    VersionMaker::MakeIncSegment(GET_PARTITION_DIRECTORY(), 6, true, 10);

    partitionData.AddSegment(6, 10);

    Version v = partitionData.GetVersion();
    Version expect = VersionMaker::Make(1, "0,2,4,6", "", "", 10);
    INDEXLIB_TEST_EQUAL(subPartitionData->GetVersion(), v);
    INDEXLIB_TEST_EQUAL(expect, v);
}

void InMemoryPartitionDataTest::TestClone()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 0, "0", "", "", 0, true);
    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSIONID), true);
    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    InMemoryPartitionDataPtr partitionData(new InMemoryPartitionData(dumpContainer));
    partitionData->Open(segDir);

    InMemoryPartitionDataPtr clonePartitionData(partitionData->Clone());
    InMemoryPartitionData* subData = clonePartitionData->GetSubInMemoryPartitionData();
    ASSERT_TRUE(clonePartitionData->GetSubPartitionData() != partitionData->GetSubPartitionData());
    ASSERT_EQ(clonePartitionData->mDumpSegmentContainer, partitionData->mDumpSegmentContainer);
    ASSERT_EQ(clonePartitionData->mDumpSegmentContainer, subData->mDumpSegmentContainer);
}

void InMemoryPartitionDataTest::TestCreateNewSegmentData()
{
    IndexPartitionOptions options;
    InMemorySegmentCreator creator;
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    creator.Init(mSchema, options);
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 1, "0,2,4", "", "", 10, true);
    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSIONID), true);

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    InMemoryPartitionData inMemPartData(dumpContainer);
    inMemPartData.Open(segDir);

    PartitionDataPtr subPartitionData = inMemPartData.GetSubPartitionData();
    INDEXLIB_TEST_TRUE(subPartitionData);
    BuildingSegmentData segData = inMemPartData.CreateNewSegmentData();
    ASSERT_EQ(5u, segData.GetSegmentId());
    InMemorySegmentPtr inMemSeg = creator.Create(
        PartitionSegmentIteratorPtr(), segData, GET_SEGMENT_DIRECTORY(),
        util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(autil::mem_pool::Pool::DEFAULT_CHUNK_SIZE),
        CounterMapPtr(), plugin::PluginManagerPtr());

    // todo : add mock SegmentDumpItem for container
    NormalSegmentDumpItemPtr item(new MockSegmentDumpItem(options, mSchema, inMemSeg));
    dumpContainer->PushDumpItem(item);
    ASSERT_EQ(5u, inMemSeg->GetSegmentId());

    BuildingSegmentData segData2 = inMemPartData.CreateNewSegmentData();
    ASSERT_EQ(6u, segData2.GetSegmentId());
    ASSERT_EQ(6u, segData2.GetSubSegmentData()->GetSegmentId());
}
}} // namespace indexlib::partition
