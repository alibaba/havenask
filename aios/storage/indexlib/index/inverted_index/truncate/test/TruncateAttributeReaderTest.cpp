#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"

#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class TruncateAttributeReaderTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(TruncateAttributeReaderTest, TestCreateReadContext)
{
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto reader = std::make_shared<TruncateAttributeReader>(docMapper, attrConfig);
    auto pool = std::make_shared<autil::mem_pool::Pool>();

    reader->AddAttributeDiskIndexer(0, nullptr);
    auto context = reader->CreateReadContextPtr(pool.get());
    ASSERT_EQ(context, nullptr);

    indexlibv2::index::DiskIndexerParameter indexerParam;
    indexerParam.docCount = 5;
    kmonitor::MetricsTags metricsTags;
    auto metricsReporter = std::make_shared<kmonitor::MetricsReporter>("", metricsTags, "");
    auto metrics = std::make_shared<indexlibv2::index::AttributeMetrics>(metricsReporter);
    auto indexer = std::make_shared<indexlibv2::index::SingleValueAttributeDiskIndexer<int64_t>>(metrics, indexerParam);
    reader->AddAttributeDiskIndexer(1, indexer);
    context = reader->CreateReadContextPtr(pool.get());
    ASSERT_NE(context, nullptr);
}

} // namespace indexlib::index
