#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/build_document_metrics.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/test/mock_clock.h"

namespace indexlib { namespace partition {

class BuildDocumentMetricsTest : public INDEXLIB_TESTBASE
{
public:
    BuildDocumentMetricsTest() {}
    ~BuildDocumentMetricsTest() {}

    DECLARE_CLASS_NAME(BuildDocumentMetricsTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}

public:
    static std::unique_ptr<BuildDocumentMetrics> CreateBuildDocumentMetrics(util::MetricProviderPtr metricProvider,
                                                                            util::Clock** clock);

private:
    IE_LOG_DECLARE();
};

std::unique_ptr<BuildDocumentMetrics>
BuildDocumentMetricsTest::CreateBuildDocumentMetrics(util::MetricProviderPtr metricProvider, util::Clock** clock)
{
    auto metrics = std::make_unique<BuildDocumentMetrics>(metricProvider, tt_index);
    metrics->RegisterMetrics();
    if (clock != nullptr) {
        auto mockClock = std::make_unique<util::MockClock>();
        *clock = mockClock.get();
        metrics->TEST_SetClock(std::move(mockClock));
    }
    return metrics;
}

IE_LOG_SETUP(partition, BuildDocumentMetricsTest);

TEST_F(BuildDocumentMetricsTest, TestAddToUpdate)
{
    std::unique_ptr<BuildDocumentMetrics> metrics =
        CreateBuildDocumentMetrics(/*metricProvider=*/nullptr, /*clock=*/nullptr);

    document::NormalDocumentPtr doc(new document::NormalDocument);
    doc->SetDocOperateType(ADD_DOC);
    doc->ModifyDocOperateType(UPDATE_FIELD);
    metrics->ReportBuildDocumentMetrics(doc, /*successMsgCount=*/1);

    ASSERT_EQ((uint32_t)1, metrics->GetSuccessDocCount());
    ASSERT_EQ((uint32_t)1, metrics->GetAddToUpdateDocCount());

    metrics->ReportBuildDocumentMetrics(doc, /*successMsgCount=*/0);

    ASSERT_EQ((uint32_t)1, metrics->GetSuccessDocCount());
    ASSERT_EQ((uint32_t)1, metrics->GetAddToUpdateDocCount());
}

TEST_F(BuildDocumentMetricsTest, TestPeriodicReport)
{
    util::Clock* clock;
    util::MetricProviderPtr metricProvider(new util::MetricProvider());
    std::unique_ptr<BuildDocumentMetrics> metrics = CreateBuildDocumentMetrics(metricProvider, &clock);

    document::NormalDocumentPtr doc(new document::NormalDocument);
    doc->SetDocOperateType(ADD_DOC);
    metrics->ReportBuildDocumentMetrics(doc, /*successMsgCount=*/1);
    EXPECT_EQ((uint32_t)1, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    clock->SleepFor(BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US + 1);

    metrics->ReportBuildDocumentMetrics(doc, /*successMsgCount=*/1);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)2, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
}

TEST_F(BuildDocumentMetricsTest, TestOverflow)
{
    util::MetricPtr successQpsMetric(
        new util::Metric(/*reporter=*/nullptr, /*metricName=*/"add", /*type=*/kmonitor::QPS));
    util::MetricPtr successDocCountMetric(
        new util::Metric(/*reporter=*/nullptr, /*metricName=*/"add", /*type=*/kmonitor::STATUS));

    uint32_t successDocCount = 0;
    uint32_t failedDocCount = 0;
    uint32_t successDocAccumulator = 0;
    uint32_t failedDocAccumulator = 0;
    successDocCountMetric->TEST_SetValue(1000);
    BuildDocumentMetrics::ReportMetricsByType(successQpsMetric, successDocCountMetric, /*failedQpsMetric=*/nullptr,
                                              /*failedDocCountMetric=*/nullptr, &successDocCount, &failedDocCount,
                                              &successDocAccumulator, &failedDocAccumulator,
                                              /*successMsgCount=*/1,
                                              /*totalMsgCount=*/1, /*shouldReport=*/true);
    EXPECT_EQ((uint32_t)1, successDocCount);
    EXPECT_EQ((uint32_t)0, failedDocCount);
}

TEST_F(BuildDocumentMetricsTest, TestOverflow2)
{
    util::Clock* clock;
    util::MetricProviderPtr metricProvider(new util::MetricProvider());
    std::unique_ptr<BuildDocumentMetrics> metrics = CreateBuildDocumentMetrics(metricProvider, &clock);

    document::NormalDocumentPtr addDoc(new document::NormalDocument);
    addDoc->SetDocOperateType(ADD_DOC);
    metrics->ReportBuildDocumentMetrics(addDoc, /*successMsgCount=*/1);
    metrics->ReportBuildDocumentMetrics(addDoc, /*successMsgCount=*/0);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addFailedDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateFailedDocCountMetric->TEST_GetValue()));

    document::NormalDocumentPtr updateDoc(new document::NormalDocument);
    updateDoc->SetDocOperateType(UPDATE_FIELD);
    clock->SleepFor(BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US + 1);
    metrics->ReportBuildDocumentMetrics(updateDoc,
                                        /*successMsgCount=*/1); // this line will be reported because it has enough
                                                                // inverval from last ReportBuildDocumentMetrics
    metrics->ReportBuildDocumentMetrics(updateDoc, /*successMsgCount=*/0);
    EXPECT_EQ((uint32_t)4, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)3, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addFailedDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_updateDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_updateDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_updateFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateFailedDocCountMetric->TEST_GetValue()));

    // Test initiailize BuildDocumentMetrics again.
    // Metrics will keep value from last time for test purpose.
    // BuildDocumentMetrics internal counters are re-initialized.
    metrics = CreateBuildDocumentMetrics(metricProvider, &clock);

    EXPECT_EQ((uint32_t)0, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)3, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_addDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_addFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addFailedDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_updateDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateFailedDocCountMetric->TEST_GetValue()));

    clock->SleepFor(BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US + 1);
    metrics->ReportBuildDocumentMetrics(addDoc,
                                        /*successMsgCount=*/1); // this line will be reported because it has enough
                                                                // inverval from last ReportBuildDocumentMetrics
    metrics->ReportBuildDocumentMetrics(addDoc, /*successMsgCount=*/0);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_addDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addFailedDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_updateDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)0, metrics->_updateFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateFailedDocCountMetric->TEST_GetValue()));

    clock->SleepFor(BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US + 1);
    metrics->ReportBuildDocumentMetrics(updateDoc,
                                        /*successMsgCount=*/1); // this line will be reported because it has enough
                                                                // inverval from last ReportBuildDocumentMetrics
    metrics->ReportBuildDocumentMetrics(updateDoc, /*successMsgCount=*/0);
    EXPECT_EQ((uint32_t)4, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)3, (uint32_t)(metrics->_totalDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_addDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_addFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_addFailedDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_updateDocCount);
    EXPECT_EQ((uint32_t)1, (uint32_t)(metrics->_updateDocCountMetric->TEST_GetValue()));
    EXPECT_EQ((uint32_t)1, metrics->_updateFailedDocCount);
    EXPECT_EQ((uint32_t)0, (uint32_t)(metrics->_updateFailedDocCountMetric->TEST_GetValue()));
}

}} // namespace indexlib::partition
