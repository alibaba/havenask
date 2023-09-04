#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::test;

namespace indexlib { namespace index {

class InMemoryIndexSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    InMemoryIndexSegmentWriterTest() {}
    ~InMemoryIndexSegmentWriterTest() {}

    DECLARE_CLASS_NAME(InMemoryIndexSegmentWriterTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, InMemoryIndexSegmentWriterTest);

TEST_F(InMemoryIndexSegmentWriterTest, TestGetAdjustHashMapInitSize)
{
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "pk:string;cfield:string", "pk:primarykey64:pk;index1:string:cfield;index2:string:cfield", "", "");

    config::IndexConfigPtr indexConfig1 = schema->GetIndexSchema()->GetIndexConfig("index1");
    config::IndexConfigPtr indexConfig2 = schema->GetIndexSchema()->GetIndexConfig("index2");
    ASSERT_TRUE(indexConfig1);
    ASSERT_TRUE(indexConfig2);

    std::shared_ptr<indexlib::framework::SegmentMetrics> segmentMetrics(new framework::SegmentMetrics);
    segmentMetrics->SetDistinctTermCount("index1", 10);
    config::IndexPartitionOptions options;

    std::unique_ptr<IndexWriter> indexWriter1 = index::IndexWriterFactory::CreateIndexWriter(
        INVALID_SEGMENTID, indexConfig1, segmentMetrics, options, /*BuildResourceMetrics=*/nullptr,
        /*PluginManager=*/nullptr, /*PartitionSegmentIterator=*/nullptr);

    std::unique_ptr<IndexWriter> indexWriter2 = index::IndexWriterFactory::CreateIndexWriter(
        INVALID_SEGMENTID, indexConfig2, segmentMetrics, options, /*BuildResourceMetrics=*/nullptr,
        /*PluginManager=*/nullptr, /*PartitionSegmentIterator=*/nullptr);

    NormalIndexWriter* typedIndexWriter1 = dynamic_cast<NormalIndexWriter*>(indexWriter1.get());
    NormalIndexWriter* typedIndexWriter2 = dynamic_cast<NormalIndexWriter*>(indexWriter2.get());

    double factor = NormalIndexWriter::HASHMAP_INIT_SIZE_FACTOR;
    EXPECT_EQ((size_t)(10 * factor), typedIndexWriter1->mHashMapInitSize);
    EXPECT_EQ((size_t)(HASHMAP_INIT_SIZE * factor), typedIndexWriter2->mHashMapInitSize);
}

}} // namespace indexlib::index
