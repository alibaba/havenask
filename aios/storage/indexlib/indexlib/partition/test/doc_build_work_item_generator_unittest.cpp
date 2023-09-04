#include "autil/ThreadPool.h"
#include "autil/WorkItemQueue.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/partition/doc_build_work_item_generator.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/partition/test/main_sub_test_util.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/GroupedThreadPool.h"

using namespace std;
using namespace indexlib::test;

namespace indexlib { namespace partition {

class DocBuildWorkItemGeneratorTest : public INDEXLIB_TESTBASE
{
public:
    DocBuildWorkItemGeneratorTest() {}
    ~DocBuildWorkItemGeneratorTest() {}

    DECLARE_CLASS_NAME(DocBuildWorkItemGeneratorTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(partition, DocBuildWorkItemGeneratorTest);

TEST_F(DocBuildWorkItemGeneratorTest, TestSimpleProcess)
{
    config::IndexPartitionSchemaPtr schema =
        test::SchemaMaker::MakeSchema("pk:int64;price:int32;", /*index=*/"pk:primarykey64:pk;price:number:price;",
                                      /*attr=*/"pk;price", /*summary=*/"");
    config::IndexPartitionSchemaPtr subSchema =
        test::SchemaMaker::MakeSchema("sub_pk:int64;sub_price:int32;",
                                      /*index=*/"sub_pk:primarykey64:sub_pk;",
                                      /*attr=*/"sub_pk;sub_price", /*summary=*/"");
    schema->SetSubIndexPartitionSchema(subSchema);

    config::IndexPartitionOptions options;

    SingleSegmentWriterPtr mainSegmentWriter(new SingleSegmentWriter(schema, options));
    SingleSegmentWriterPtr subSegmentWriter(new SingleSegmentWriter(subSchema, options));
    index_base::SegmentWriterPtr segmentWriter(
        new MainSubTestUtil::MockSubDocSegmentWriter(schema, options, mainSegmentWriter, subSegmentWriter));

    PartitionModifierPtr mainModifier(new InplaceModifier(schema));
    PartitionModifierPtr subModifier(new InplaceModifier(subSchema));
    PartitionModifierPtr modifier(new MainSubTestUtil::MockSubDocModifier(schema, mainModifier, subModifier));

    document::DocumentCollectorPtr docCollector(new document::DocumentCollector(options));

    auto queueFactory = std::make_shared<autil::ThreadPoolQueueFactory>();
    auto threadPool = std::make_shared<autil::ThreadPool>(4, size_t(-1), queueFactory, "name");
    {
        util::GroupedThreadPool groupedThreadPool;
        auto status = groupedThreadPool.Start(threadPool, /*maxGroupCount=*/1024, /*maxBatchCount=*/8);
        ASSERT_TRUE(status.IsOK());
        DocBuildWorkItemGenerator::Generate(PartitionWriter::BuildMode::BUILD_MODE_INCONSISTENT_BATCH, schema,
                                            segmentWriter, modifier, docCollector, &groupedThreadPool);

        auto workItems = groupedThreadPool.TEST_GetWorkItems();
        std::vector<std::string> actualWorkItems;
        for (auto iter = workItems->begin(); iter != workItems->end(); ++iter) {
            actualWorkItems.push_back(iter->first);
            ASSERT_EQ(1, iter->second.size());
        }
        EXPECT_THAT(actualWorkItems,
                    UnorderedElementsAre("_ATTRIBUTE_pk", "_ATTRIBUTE_price", "_INVERTEDINDEX_price_@_0",
                                         "_SUB_ATTRIBUTE_sub_pk", "_SUB_ATTRIBUTE_sub_price"));
    }
    {
        util::GroupedThreadPool groupedThreadPool;
        auto status = groupedThreadPool.Start(threadPool, /*maxGroupCount=*/1024, /*maxBatchCount=*/8);
        ASSERT_TRUE(status.IsOK());
        DocBuildWorkItemGenerator::Generate(PartitionWriter::BuildMode::BUILD_MODE_CONSISTENT_BATCH, schema,
                                            segmentWriter, modifier, docCollector, &groupedThreadPool);

        auto workItems = groupedThreadPool.TEST_GetWorkItems();
        std::vector<std::string> actualWorkItems;
        for (auto iter = workItems->begin(); iter != workItems->end(); ++iter) {
            actualWorkItems.push_back(iter->first);
            ASSERT_EQ(1, iter->second.size());
        }
        EXPECT_THAT(actualWorkItems, UnorderedElementsAre("_ATTRIBUTE_pk", "_ATTRIBUTE_price", "_INVERTEDINDEX_pk_@_0",
                                                          "_INVERTEDINDEX_price_@_0", "_SUB_ATTRIBUTE_sub_pk",
                                                          "_SUB_ATTRIBUTE_sub_price", "_SUB_INVERTEDINDEX_sub_pk_@_0"));
    }
}

}} // namespace indexlib::partition
