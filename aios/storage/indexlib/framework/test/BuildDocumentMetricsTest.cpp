#include "indexlib/framework/BuildDocumentMetrics.h"

#include "indexlib/framework/mock/MockDocumentBatch.h"
#include "kmonitor/client/KMonitorFactory.h"
#include "unittest/unittest.h"
namespace indexlibv2::framework {

class BuildDocumentMetricsTest : public TESTBASE
{
public:
    BuildDocumentMetricsTest() {}
    ~BuildDocumentMetricsTest() {}

public:
    void SetUp() override
    {
        kmonitor::MetricsConfig config;
        config.set_inited(true);
        kmonitor::KMonitorFactory::Init(config);
        kmonitor::KMonitorFactory::Start();
        kmonitor::KMonitorPtr commonMonitor(
            kmonitor::KMonitorFactory::GetKMonitor("commonMonitor", true),
            [](kmonitor::KMonitor* p) { kmonitor::KMonitorFactory::ReleaseKMonitor("commonMonitor"); });
        kmonitor::MetricsTags baseTags {{{"host", "127.0.0.1"}}};
        _reporter = std::make_shared<kmonitor::MetricsReporter>("test", "common", baseTags, "");
    }
    void TearDown() override {}

private:
    std::shared_ptr<kmonitor::MetricsReporter> _reporter;
};

TEST_F(BuildDocumentMetricsTest, TestAdd)
{
    std::unique_ptr<BuildDocumentMetrics> metrics = std::make_unique<BuildDocumentMetrics>("test", nullptr);
    metrics->RegisterMetrics();
    std::shared_ptr<MockDocument> mockDoc(new MockDocument);
    EXPECT_CALL(*mockDoc, GetDocOperateType()).WillRepeatedly(Return(ADD_DOC));
    indexlibv2::framework::MockDocumentBatch batch;
    batch.SetDocs({1, -1});
    EXPECT_CALL(batch, BracketOp(_)).WillRepeatedly(Return(mockDoc));
    metrics->ReportBuildDocumentMetrics(&batch);
    ASSERT_EQ((uint32_t)1, metrics->_successDocCount);

    metrics->ReportBuildDocumentMetrics(&batch);
    ASSERT_EQ((uint32_t)2, metrics->_successDocCount);
}

TEST_F(BuildDocumentMetricsTest, TestBuild)
{
    auto ADD = BuildDocumentMetrics::ADD;
    auto UPDATE = BuildDocumentMetrics::UPDATE;
    auto DELETE = BuildDocumentMetrics::DELETE;
    auto SUCCESS = BuildDocumentMetrics::SUCCESS;
    auto FAILED = BuildDocumentMetrics::FAILED;
    auto COUNT = BuildDocumentMetrics::COUNT;

    std::unique_ptr<BuildDocumentMetrics> metrics = std::make_unique<BuildDocumentMetrics>("test", _reporter);
    metrics->RegisterMetrics();
    std::shared_ptr<MockDocument> mockDoc(new MockDocument);
    EXPECT_CALL(*mockDoc, GetDocOperateType())
        .WillOnce(Return(ADD_DOC))
        .WillOnce(Return(DELETE_DOC))
        .WillOnce(Return(UPDATE_FIELD))
        .WillOnce(Return(ADD_DOC))
        .WillOnce(Return(DELETE_DOC))
        .WillOnce(Return(UPDATE_FIELD));
    indexlibv2::framework::MockDocumentBatch batch;
    batch.SetDocs({1, -1, 1});
    EXPECT_CALL(batch, BracketOp(_)).WillRepeatedly(Return(mockDoc));
    metrics->ReportBuildDocumentMetrics(&batch);
    ASSERT_EQ((uint32_t)2, metrics->_successDocCount);
    ASSERT_EQ(1, metrics->_metrics[ADD][SUCCESS][COUNT]._count);
    ASSERT_EQ(0, metrics->_metrics[ADD][FAILED][COUNT]._count);
    ASSERT_EQ(0, metrics->_metrics[DELETE][SUCCESS][COUNT]._count);
    ASSERT_EQ(1, metrics->_metrics[DELETE][FAILED][COUNT]._count);
    ASSERT_EQ(1, metrics->_metrics[UPDATE][SUCCESS][COUNT]._count);
    ASSERT_EQ(0, metrics->_metrics[UPDATE][FAILED][COUNT]._count);

    batch.SetDocs({-1, -1, -1});
    metrics->ReportBuildDocumentMetrics(&batch);
    ASSERT_EQ((uint32_t)2, metrics->_successDocCount);
    ASSERT_EQ(1, metrics->_metrics[ADD][SUCCESS][COUNT]._count);
    ASSERT_EQ(1, metrics->_metrics[ADD][FAILED][COUNT]._count);
    ASSERT_EQ(0, metrics->_metrics[DELETE][SUCCESS][COUNT]._count);
    ASSERT_EQ(2, metrics->_metrics[DELETE][FAILED][COUNT]._count);
    ASSERT_EQ(1, metrics->_metrics[UPDATE][SUCCESS][COUNT]._count);
    ASSERT_EQ(1, metrics->_metrics[UPDATE][FAILED][COUNT]._count);
}

TEST_F(BuildDocumentMetricsTest, TestPeriodicReport)
{
    std::unique_ptr<BuildDocumentMetrics> metrics = std::make_unique<BuildDocumentMetrics>("test", _reporter);
    metrics->_lastBuildDocumentMetricsReportTimeUS = autil::TimeUtility::currentTimeInMicroSeconds();
    metrics->RegisterMetrics();

    std::shared_ptr<MockDocument> mockDoc(new MockDocument);
    EXPECT_CALL(*mockDoc, GetDocOperateType()).WillRepeatedly(Return(ADD_DOC));

    indexlibv2::framework::MockDocumentBatch batch;
    batch.SetDocs({1, -1});
    EXPECT_CALL(batch, BracketOp(_)).WillRepeatedly(Return(mockDoc));
    metrics->ReportBuildDocumentMetrics(&batch);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocCountMetric._count);
    EXPECT_EQ((uint32_t)2, metrics->_totalBuildQpsMetric._count);
    usleep(2 * BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US);

    metrics->ReportBuildDocumentMetrics(&batch);
    EXPECT_EQ((uint32_t)4, metrics->_totalDocCountMetric._count);
    EXPECT_EQ((uint32_t)0, metrics->_totalBuildQpsMetric._count);
}

} // namespace indexlibv2::framework
