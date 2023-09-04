#include "indexlib/framework/MetricsManager.h"

#include "unittest/unittest.h"

namespace indexlibv2::framework {

class MetricsManagerTest : public TESTBASE
{
public:
    MetricsManagerTest() = default;
    ~MetricsManagerTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void MetricsManagerTest::setUp() {}

void MetricsManagerTest::tearDown() {}

uint32_t globalReportTime = 0;

class DummyMetrics : public IMetrics
{
public:
    void ReportMetrics() override
    {
        globalReportTime++;
        _reportTime++;
    }
    void RegisterMetrics() override {}
    uint32_t GetReportTime() { return _reportTime; }

private:
    uint32_t _reportTime = 0;
};

TEST_F(MetricsManagerTest, testCreate)
{
    std::function<std::shared_ptr<IMetrics>()> creator = [] { return std::make_shared<DummyMetrics>(); };
    std::shared_ptr<kmonitor::MetricsReporter> reporter;

    MetricsManager manager("", reporter);
    auto metrics = std::dynamic_pointer_cast<DummyMetrics>(manager.CreateMetrics("id1", creator));
    ASSERT_TRUE(metrics);
    manager.ReportMetrics();
    ASSERT_EQ(1, globalReportTime);

    metrics.reset();
    // report trigger recycle
    manager.ReportMetrics();
    ASSERT_EQ(1, globalReportTime);
    metrics = std::dynamic_pointer_cast<DummyMetrics>(manager.CreateMetrics("id1", creator));
    ASSERT_EQ(0, metrics->GetReportTime());

    // report once
    manager.ReportMetrics();
    ASSERT_EQ(2, globalReportTime);
    ASSERT_EQ(1, metrics->GetReportTime());

    metrics.reset();
    metrics = std::dynamic_pointer_cast<DummyMetrics>(manager.CreateMetrics("id1", creator));
    // create trigger recycle
    ASSERT_EQ(0, metrics->GetReportTime());
}
} // namespace indexlibv2::framework
