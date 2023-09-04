#include "indexlib/framework/BuildDocumentMetrics.h"

#include "indexlib/framework/mock/MockDocumentBatch.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class BuildDocumentMetricsTest : public TESTBASE
{
public:
    BuildDocumentMetricsTest() {}
    ~BuildDocumentMetricsTest() {}

public:
    void SetUp() override {}
    void TearDown() override {}
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
    ASSERT_EQ((uint32_t)1, metrics->GetSuccessDocCount());

    metrics->ReportBuildDocumentMetrics(&batch);
    ASSERT_EQ((uint32_t)2, metrics->GetSuccessDocCount());
}

TEST_F(BuildDocumentMetricsTest, TestPeriodicReport)
{
    std::unique_ptr<BuildDocumentMetrics> metrics = std::make_unique<BuildDocumentMetrics>("test", nullptr);
    metrics->RegisterMetrics();

    std::shared_ptr<MockDocument> mockDoc(new MockDocument);
    EXPECT_CALL(*mockDoc, GetDocOperateType()).WillRepeatedly(Return(ADD_DOC));

    indexlibv2::framework::MockDocumentBatch batch;
    batch.SetDocs({1, -1});
    EXPECT_CALL(batch, BracketOp(_)).WillRepeatedly(Return(mockDoc));
    metrics->ReportBuildDocumentMetrics(&batch);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)2, metrics->_totalDocAccumulator);
    usleep(2 * BuildDocumentMetrics::DEFAULT_REPORT_INTERVAL_US);

    metrics->ReportBuildDocumentMetrics(&batch);
    EXPECT_EQ((uint32_t)4, metrics->_totalDocCount);
    EXPECT_EQ((uint32_t)0, metrics->_totalDocAccumulator);
}

} // namespace indexlibv2::framework
