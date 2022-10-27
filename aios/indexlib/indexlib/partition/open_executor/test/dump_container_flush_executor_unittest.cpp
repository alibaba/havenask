#include "indexlib/partition/open_executor/test/dump_container_flush_executor_unittest.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/partition/index_builder.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(open_executor, DumpContainerFlushExecutorTest);

namespace{
    class MockSegmentDumpItem : public NormalSegmentDumpItem
    {
    public:
        MockSegmentDumpItem(
                const config::IndexPartitionOptions& options,
                const config::IndexPartitionSchemaPtr& schema,
                const std::string& partitionName)
            : NormalSegmentDumpItem(options, schema, partitionName)
        {
        }
        ~MockSegmentDumpItem()
        {
        }
        bool DumpWithMemLimit() override
        {
            return false;
        }
    };
}

DumpContainerFlushExecutorTest::DumpContainerFlushExecutorTest()
{
}

DumpContainerFlushExecutorTest::~DumpContainerFlushExecutorTest()
{
}

void DumpContainerFlushExecutorTest::CaseSetUp()
{
    string field = "string1:string;";
    string index = "pk:primarykey64:string1";    
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexPartitionOptions options;
    QuotaControlPtr quotaControl(new QuotaControl(1024 * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(GET_TEST_DATA_PATH(), options, 
                                              mSchema, quotaControl));
    builder->Init();
    builder->EndIndex();
    options.SetIsOnline(true);
    mPartition.reset(new OnlinePartition());
    mPartition->Open(GET_TEST_DATA_PATH(), "", mSchema, options);
    mPartition->ReOpenNewSegment();
}

void DumpContainerFlushExecutorTest::CaseTearDown()
{
}

void DumpContainerFlushExecutorTest::TestSimpleProcess()
{
    //pre dump
    //给一个增量的partitionData，能够过滤掉过期的segment
    //给一个dump container， 能够把dump container里面所有的segment都刷出去,原来的container保持不变。
    //dump
    //和原来的container做一次合并，并把所有的dumping segment刷出去
    //drop
    //原来刷出去的索引恢复原样
    IndexPartitionOptions options;
    
    DumpSegmentContainerPtr dumpContainer = mPartition->mDumpSegmentContainer;
    ScopedLock lock(mPartition->mCleanerLock);
    PartitionDataPtr partData = mPartition->GetPartitionData();
    InMemorySegmentPtr inMemSeg = partData->CreateNewSegment();
    string docString = "cmd=add,string1=1,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    inMemSeg->GetSegmentWriter()->AddDocument(doc);
    NormalSegmentDumpItemPtr dumpItem(new NormalSegmentDumpItem(options, mSchema, ""));
    dumpItem->Init(NULL, partData, PartitionModifierPtr(), FlushedLocatorContainerPtr(), true);
    dumpContainer->PushDumpItem(dumpItem);
    Version invalidVersion;
    ExecutorResource resource = mPartition->CreateExecutorResource(invalidVersion, false);
    DumpContainerFlushExecutor executor(&mPartition->mDataLock, "");
    ASSERT_TRUE(executor.Execute(resource));
    ASSERT_EQ(0u, dumpContainer->GetDumpItemSize());
    ASSERT_EQ(inMemSeg->GetSegmentId(), resource.mPartitionDataHolder.Get()->GetVersion().GetLastSegment());
}

void DumpContainerFlushExecutorTest::TestExecuteFailed()
{
    Version invalidVersion;
    ExecutorResource resource = mPartition->CreateExecutorResource(invalidVersion, false);
    DumpContainerFlushExecutor executor(&mPartition->mDataLock, "");
    IndexPartitionOptions options;
    DumpSegmentContainerPtr dumpContainer = mPartition->mDumpSegmentContainer;
    
    ScopedLock lock(mPartition->mCleanerLock);
    PartitionDataPtr partData = mPartition->GetPartitionData();
    InMemorySegmentPtr inMemSeg = partData->CreateNewSegment();
    string docString = "cmd=add,string1=1,ts=1";
    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    ASSERT_TRUE(executor.Execute(resource));
    ASSERT_EQ(INVALID_SEGMENTID, resource.mPartitionDataHolder.Get()->GetVersion().GetLastSegment());
    inMemSeg->GetSegmentWriter()->AddDocument(doc);
    NormalSegmentDumpItemPtr dumpItem(new NormalSegmentDumpItem(options, mSchema, ""));
    dumpItem->Init(NULL, partData, PartitionModifierPtr(), FlushedLocatorContainerPtr(), true);
    dumpContainer->PushDumpItem(dumpItem);

    InMemorySegmentPtr inMemSeg2 = partData->CreateNewSegment();
    NormalSegmentDumpItemPtr dumpItem2(new MockSegmentDumpItem(options, mSchema, ""));
    dumpItem2->Init(NULL, partData, PartitionModifierPtr(), FlushedLocatorContainerPtr(), true);
    dumpContainer->PushDumpItem(dumpItem2);

    InMemorySegmentPtr inMemSeg3 = partData->CreateNewSegment();
    NormalSegmentDumpItemPtr dumpItem3(new NormalSegmentDumpItem(options, mSchema, ""));
    dumpItem3->Init(NULL, partData, PartitionModifierPtr(), FlushedLocatorContainerPtr(), true);
    inMemSeg->GetSegmentWriter()->AddDocument(doc);
    dumpContainer->PushDumpItem(dumpItem3);

    ASSERT_FALSE(executor.Execute(resource));
    ASSERT_EQ(2u, dumpContainer->GetDumpItemSize());
    ASSERT_EQ(inMemSeg->GetSegmentId(), resource.mPartitionDataHolder.Get()->GetVersion().GetLastSegment());
}

IE_NAMESPACE_END(partition);

