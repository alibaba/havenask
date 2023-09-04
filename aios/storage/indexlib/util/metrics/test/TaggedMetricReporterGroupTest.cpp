#include "indexlib/util/metrics/TaggedMetricReporterGroup.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class TaggedMetricReporterGroupTest : public INDEXLIB_TESTBASE
{
public:
    TaggedMetricReporterGroupTest();
    ~TaggedMetricReporterGroupTest();

    DECLARE_CLASS_NAME(TaggedMetricReporterGroupTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLimitTagKeys();
};

INDEXLIB_UNIT_TEST_CASE(TaggedMetricReporterGroupTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(TaggedMetricReporterGroupTest, TestLimitTagKeys);

TaggedMetricReporterGroupTest::TaggedMetricReporterGroupTest() {}

TaggedMetricReporterGroupTest::~TaggedMetricReporterGroupTest() {}

void TaggedMetricReporterGroupTest::CaseSetUp() {}

void TaggedMetricReporterGroupTest::CaseTearDown() {}

void TaggedMetricReporterGroupTest::TestSimpleProcess()
{
    ASSERT_EQ(kmonitor::GAUGE, InputTaggedMetricReporterGroup::GetMetricType());
    ASSERT_EQ(kmonitor::QPS, QpsTaggedMetricReporterGroup::GetMetricType());

    map<string, string> tagMap = {{"k1", "v1"}, {"k2", "v2"}};
    QpsTaggedMetricReporterGroup reporterGroup;
    auto iter1 = reporterGroup.DeclareMetricReporter(tagMap);
    auto iter2 = reporterGroup.DeclareMetricReporter(tagMap);

    ASSERT_EQ(iter1.get(), iter2.get());
    reporterGroup.Report();
    ASSERT_EQ(1, reporterGroup.GetReporterSize());

    iter1.reset();
    reporterGroup.Report();
    ASSERT_EQ(1, reporterGroup.GetReporterSize());

    iter2.reset();
    reporterGroup.Report();
    ASSERT_EQ(0, reporterGroup.GetReporterSize());
}

void TaggedMetricReporterGroupTest::TestLimitTagKeys()
{
    vector<string> limitKeys;
    limitKeys = {"k1"};
    map<string, string> tagMap1 = {{"k1", "v1"}, {"k2", "v2"}};
    map<string, string> tagMap2 = {{"k1", "v1"}, {"k2", "v3"}};
    QpsTaggedMetricReporterGroup reporterGroup;
    reporterGroup.Init(util::MetricProviderPtr(), "indexlib", limitKeys);
    auto iter1 = reporterGroup.DeclareMetricReporter(tagMap1);
    auto iter2 = reporterGroup.DeclareMetricReporter(tagMap2);

    ASSERT_EQ(iter1.get(), iter2.get());
    reporterGroup.Report();
    ASSERT_EQ(1, reporterGroup.GetReporterSize());
}

}} // namespace indexlib::util
