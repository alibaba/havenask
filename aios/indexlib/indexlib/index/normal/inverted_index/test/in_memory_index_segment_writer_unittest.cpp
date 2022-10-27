#include "indexlib/index/normal/inverted_index/test/in_memory_index_segment_writer_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemoryIndexSegmentWriterTest);

InMemoryIndexSegmentWriterTest::InMemoryIndexSegmentWriterTest()
{
}

InMemoryIndexSegmentWriterTest::~InMemoryIndexSegmentWriterTest()
{
}

void InMemoryIndexSegmentWriterTest::CaseSetUp()
{
}

void InMemoryIndexSegmentWriterTest::CaseTearDown()
{
}

void InMemoryIndexSegmentWriterTest::TestGetAdjustHashMapInitSize()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "pk:string;cfield:string",
            "pk:primarykey64:pk;index1:string:cfield;index2:string:cfield",
            "",
            "");

    IndexConfigPtr indexConfig1 = schema->GetIndexSchema()->GetIndexConfig("index1");
    IndexConfigPtr indexConfig2 = schema->GetIndexSchema()->GetIndexConfig("index2");    
    ASSERT_TRUE(indexConfig1);
    ASSERT_TRUE(indexConfig2);    

    SegmentMetricsPtr metrics(new SegmentMetrics);
    metrics->SetDistinctTermCount("index1", 10);

    InMemoryIndexSegmentWriter indexSegWriter;
    indexSegWriter.mSegmentMetrics = metrics;
    IndexPartitionOptions options;

    IndexWriterPtr indexWriter1 = indexSegWriter.CreateIndexWriter(
            indexConfig1, options, NULL,
            plugin::PluginManagerPtr());
    IndexWriterPtr indexWriter2 = indexSegWriter.CreateIndexWriter(
            indexConfig2, options, NULL,
            plugin::PluginManagerPtr()); 

    auto typedIndexWriter1 = dynamic_pointer_cast<NormalIndexWriter>(indexWriter1);
    auto typedIndexWriter2 = dynamic_pointer_cast<NormalIndexWriter>(indexWriter2);    
    
    double factor = NormalIndexWriter::HASHMAP_INIT_SIZE_FACTOR;
    INDEXLIB_TEST_EQUAL((size_t)(10 * factor), typedIndexWriter1->mHashMapInitSize);
    INDEXLIB_TEST_EQUAL((size_t)(HASHMAP_INIT_SIZE * factor), typedIndexWriter2->mHashMapInitSize);
}


IE_NAMESPACE_END(index);

