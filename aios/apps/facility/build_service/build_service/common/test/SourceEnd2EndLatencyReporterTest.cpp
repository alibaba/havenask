#include "build_service/common/SourceEnd2EndLatencyReporter.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "build_service/test/unittest.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricType.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;

namespace build_service::common {
class SourceEnd2EndLatencyReporterTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(SourceEnd2EndLatencyReporterTest, testInit)
{
    SourceEnd2EndLatencyReporter reporter;
    auto metricProvider = std::make_shared<MetricProvider>(nullptr);
    auto schema = std::make_shared<IndexPartitionSchema>();
    IndexPartitionSchemaMaker::MakeSchema(schema, "f1:LONG;f2:LONG;f3:LONG;f4:LONG;", "pk:primarykey64:f3", "", "");
    auto fieldConfig = schema->GetFieldConfig("f1");
    autil::legacy::json::JsonMap jm1;
    jm1["source_timestamp_report_key"] = autil::legacy::Any(std::string("source_1"));
    fieldConfig->SetUserDefinedParam(jm1);

    autil::legacy::json::JsonMap jm2;
    jm2["source_timestamp_report_key"] = autil::legacy::Any(std::string("source_2"));
    jm2["source_timestamp_unit"] = autil::legacy::Any(std::string("ms"));
    fieldConfig = schema->GetFieldConfig("f2");
    fieldConfig->SetUserDefinedParam(jm2);

    autil::legacy::json::JsonMap jm3;
    jm3["source_timestamp_report_key"] = autil::legacy::Any(std::string("source_3"));
    jm3["source_timestamp_unit"] = autil::legacy::Any(std::string("us"));
    fieldConfig = schema->GetFieldConfig("f3");
    fieldConfig->SetUserDefinedParam(jm3);

    autil::legacy::json::JsonMap jm4;
    jm4["source_timestamp_report_key"] = autil::legacy::Any(std::string("source_4"));
    jm4["source_timestamp_unit"] = autil::legacy::Any(std::string("sec"));
    fieldConfig = schema->GetFieldConfig("f4");
    fieldConfig->SetUserDefinedParam(jm4);

    reporter.init(metricProvider, schema);
    ASSERT_EQ(reporter._sourceE2ELatencyTagMap.size(), 4);
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap.find("source_1") != reporter._sourceE2ELatencyTagMap.end());
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap["source_1"].timestampUnit ==
                SourceEnd2EndLatencyReporter::TimestampUnit::TT_US);
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap.find("source_2") != reporter._sourceE2ELatencyTagMap.end());
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap["source_2"].timestampUnit ==
                SourceEnd2EndLatencyReporter::TimestampUnit::TT_MS);
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap.find("source_3") != reporter._sourceE2ELatencyTagMap.end());
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap["source_3"].timestampUnit ==
                SourceEnd2EndLatencyReporter::TimestampUnit::TT_US);
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap.find("source_4") != reporter._sourceE2ELatencyTagMap.end());
    ASSERT_TRUE(reporter._sourceE2ELatencyTagMap["source_4"].timestampUnit ==
                SourceEnd2EndLatencyReporter::TimestampUnit::TT_SEC);
}

TEST_F(SourceEnd2EndLatencyReporterTest, testReport)
{
    SourceEnd2EndLatencyReporter reporter;
    reporter._sourceE2ELatencyMetric =
        std::make_shared<indexlib::util::Metric>(nullptr, "source_name", kmonitor::MetricType::GAUGE);
    {
        reporter._sourceE2ELatencyTagMap["source_name"] = {SourceEnd2EndLatencyReporter::TimestampUnit::TT_US, nullptr,
                                                           nullptr};
        reporter.report(string("source_name|1001000;"), 1100000, true);
        ASSERT_EQ(reporter._sourceE2ELatencyMetric->TEST_GetValue(), 99);
    }
    {
        reporter._sourceE2ELatencyTagMap["source_name"] = {SourceEnd2EndLatencyReporter::TimestampUnit::TT_MS, nullptr,
                                                           nullptr};
        reporter.report(string("source_name|1001;"), 1100000, true);
        ASSERT_EQ(reporter._sourceE2ELatencyMetric->TEST_GetValue(), 99);
    }
    {
        reporter._sourceE2ELatencyTagMap["source_name"] = {SourceEnd2EndLatencyReporter::TimestampUnit::TT_SEC, nullptr,
                                                           nullptr};
        reporter.report(string("source_name|1001;"), 1100000000, false);
        ASSERT_EQ(reporter._sourceE2ELatencyMetric->TEST_GetValue(), 99000);
    }
}

TEST_F(SourceEnd2EndLatencyReporterTest, testStrToTimestampUnit)
{
    std::string str = "sec";
    ASSERT_EQ(SourceEnd2EndLatencyReporter::StrToTimestampUnit(str),
              SourceEnd2EndLatencyReporter::TimestampUnit::TT_SEC);
    str = "ms";
    ASSERT_EQ(SourceEnd2EndLatencyReporter::StrToTimestampUnit(str),
              SourceEnd2EndLatencyReporter::TimestampUnit::TT_MS);
    str = "us";
    ASSERT_EQ(SourceEnd2EndLatencyReporter::StrToTimestampUnit(str),
              SourceEnd2EndLatencyReporter::TimestampUnit::TT_US);
    str = "invalid";
    ASSERT_EQ(SourceEnd2EndLatencyReporter::StrToTimestampUnit(str),
              SourceEnd2EndLatencyReporter::TimestampUnit::TT_UNKNOWN);
}

TEST_F(SourceEnd2EndLatencyReporterTest, testConvertToUS)
{
    int64_t timestamp = 10001;
    ASSERT_EQ(SourceEnd2EndLatencyReporter::ConvertToUS(timestamp, SourceEnd2EndLatencyReporter::TimestampUnit::TT_MS),
              10001000);
    ASSERT_EQ(SourceEnd2EndLatencyReporter::ConvertToUS(timestamp, SourceEnd2EndLatencyReporter::TimestampUnit::TT_US),
              10001);
    ASSERT_EQ(SourceEnd2EndLatencyReporter::ConvertToUS(timestamp, SourceEnd2EndLatencyReporter::TimestampUnit::TT_SEC),
              10001000000);
    ASSERT_EQ(
        SourceEnd2EndLatencyReporter::ConvertToUS(timestamp, SourceEnd2EndLatencyReporter::TimestampUnit::TT_UNKNOWN),
        SourceEnd2EndLatencyReporter::INVALID_TIMESTAMP);
}

} // namespace build_service::common