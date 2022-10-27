#include "indexlib/partition/test/index_builder_unittest.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/document/document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilderTest);

namespace
{
class MockOfflinePartition : public OfflinePartition
{
public:
    MockOfflinePartition(const PartitionDataPtr& partData,
                         const IndexPartitionSchemaPtr& schema)
    {
        mPartitionDataHolder = partData;
        mSchema = schema;
    }

    MOCK_CONST_METHOD0(GetWriter, PartitionWriterPtr());
    MOCK_METHOD0(Close, void());
    MOCK_METHOD0(ReOpenNewSegment, void());
};

class MockOfflinePartitionWriter : public OfflinePartitionWriter
{
public:
    MockOfflinePartitionWriter(
            const config::IndexPartitionOptions& options)
        : OfflinePartitionWriter(options, DumpSegmentContainerPtr(new DumpSegmentContainer))
    {}
    
    MOCK_METHOD1(BuildDocument, bool(const document::DocumentPtr& doc));
    MOCK_CONST_METHOD1(NeedDump, bool(size_t));
    MOCK_METHOD0(DumpSegment, void());
    MOCK_METHOD0(Close, void());
};
}

IndexBuilderTest::IndexBuilderTest()
{
}

IndexBuilderTest::~IndexBuilderTest()
{
}

void IndexBuilderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema.reset(new IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(mSchema,
            //Field schema
            "string0:string;string1:string;long1:uint32;string2:string:true", 
            //Index schema
            "string2:string:string2",
            //Attribute schema
            "long1;string0;string1;string2",
            //Summary schema
            "string1" );
}

void IndexBuilderTest::CaseTearDown()
{
}

void IndexBuilderTest::TestAddDocument()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    
    MockOfflinePartitionWriter* mockWriter = 
            new MockOfflinePartitionWriter(options);
    OfflinePartitionWriterPtr offlineWriter(mockWriter);

    PartitionDataPtr partData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    MockOfflinePartition* mockPartition = 
        new MockOfflinePartition(partData, mSchema);
    IndexPartitionPtr indexPartition(mockPartition);
    EXPECT_CALL(*mockPartition, GetWriter())
        .WillOnce(Return(offlineWriter));

    util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
    IndexBuilder builder(indexPartition, quotaControl);
    INDEXLIB_TEST_TRUE(builder.Init());
    DocumentPtr doc;

    {
        // add false
        EXPECT_CALL(*mockWriter, BuildDocument(_))
            .WillOnce(Return(false));
        ASSERT_FALSE(builder.BuildDocument(doc));
    }

    {
        // add ok, no need dump
        EXPECT_CALL(*mockWriter, BuildDocument(_))
            .WillOnce(Return(true));
        EXPECT_CALL(*mockWriter, NeedDump(_))
            .WillOnce(Return(false));
        ASSERT_TRUE(builder.BuildDocument(doc));
    }

    {
        // add ok, need dump
        EXPECT_CALL(*mockWriter, BuildDocument(_))
            .WillOnce(Return(true));
        EXPECT_CALL(*mockWriter, NeedDump(_))
            .WillOnce(Return(true));
        EXPECT_CALL(*mockWriter, DumpSegment())
            .WillOnce(Return());
        EXPECT_CALL(*mockPartition, ReOpenNewSegment())
            .WillOnce(Return());
        ASSERT_TRUE(builder.BuildDocument(doc));
    }
}

void IndexBuilderTest::TestGetLocatorInLatesetVersion()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);

    util::QuotaControlPtr quotaControl(new util::QuotaControl(1024 * 1024 * 1024));
    IndexBuilder indexBuilder(GET_TEST_DATA_PATH(), options, mSchema, quotaControl);
    indexBuilder.Init();
    indexBuilder.EndIndex();

    ASSERT_EQ(string(""), indexBuilder.GetLocatorInLatestVersion());
    
    PartitionDataMaker::MakePartitionDataFiles(1, 1,
            GET_PARTITION_DIRECTORY(), "1,1,1;2,2,2;3,3,3");
    ASSERT_EQ(string("3"), indexBuilder.GetLocatorInLatestVersion());

    PartitionDataMaker::MakePartitionDataFiles(2, 2,
            GET_PARTITION_DIRECTORY(), "4,4,4");
    ASSERT_EQ(string("4"), indexBuilder.GetLocatorInLatestVersion());
    
    GET_PARTITION_DIRECTORY()->RemoveFile("version.2");
    ASSERT_EQ(string("3"), indexBuilder.GetLocatorInLatestVersion());

    GET_PARTITION_DIRECTORY()->RemoveFile("segment_3_level_0/segment_info");
    ASSERT_THROW(indexBuilder.GetLocatorInLatestVersion(), 
                 misc::IndexCollapsedException);
}

void IndexBuilderTest::TestUpdateDcoumentFailedWhenAsyncDump()
{
    string field = "pk:uint64:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint64:true;"
                   "updatable_multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    IndexPartitionOptions options;
    DumpSegmentContainerPtr dumpContainer(new SlowDumpSegmentContainer(1000));
    options.GetOnlineConfig().enableAsyncDumpSegment = true;
    PartitionStateMachine psm(DEFAULT_MEMORY_USE_LIMIT, false,
                              partition::IndexPartitionResource(), dumpContainer);
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    string rtDocs = "cmd=add,pk=1,long1=1,updatable_multi_long=1 10,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocs, "pk:1",
                                    "docid=0,long1=1,updatable_multi_long=1 10"));
    string incDocs = "cmd=update_field,pk=1,long1=4,updatable_multi_long=4 40,ts=4;";
    ASSERT_EQ(1u, dumpContainer->GetDumpItemSize());
    ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "pk: 1",
                    "docid=0,long1=4,updatable_multi_long=4 40"));
    sleep(5);
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());    
}

IE_NAMESPACE_END(partition);

