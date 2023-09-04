#include "indexlib/index/test/time_series_segment_metrics_updater_unittest.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/segment_metrics_updater/time_series_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::index;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TimeSeriesSegmentMetricsUpdaterTest);

TimeSeriesSegmentMetricsUpdaterTest::TimeSeriesSegmentMetricsUpdaterTest() {}

TimeSeriesSegmentMetricsUpdaterTest::~TimeSeriesSegmentMetricsUpdaterTest() {}

void TimeSeriesSegmentMetricsUpdaterTest::CaseSetUp() {}

void TimeSeriesSegmentMetricsUpdaterTest::CaseTearDown() {}

void TimeSeriesSegmentMetricsUpdaterTest::TestSimpleProcess()
{
    string field = "pk:uint64:pk;string1:string;long1:uint32;double1:double;";
    string index = "pk:primarykey64:pk;";
    string attr = "string1;long1;double1";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
    auto attrSchema = schema->GetAttributeSchema();
    IndexPartitionOptions options;
    assert(attrSchema);
    {
        util::KeyValueMap parameters;
        parameters["attribute_name"] = "long1";
        parameters["period"] = "100";
        parameters["min_doc_count"] = "2";
        parameters["continuous_len"] = "2";

        TimeSeriesSegmentMetricsUpdater updater(nullptr);
        ASSERT_TRUE(updater.Init(schema, options, parameters));
        string docStrings = "cmd=add,pk=1,string1=A,long1=9999999,double1=1.1;"
                            "cmd=add,pk=2,string1=B,long1=9999999,double1=1.3;"
                            "cmd=add,pk=3,string1=C,long1=99999,double1=1.5;"
                            "cmd=add,pk=4,string1=D,long1=99988,double1=1.5;"
                            "cmd=add,pk=5,string1=L,long1=419,double1=1.5;"
                            "cmd=add,pk=7,string1=L,long1=99811,double1=1.5;"
                            "cmd=add,pk=6,string1=E,long1=88877,double1=1.5;"
                            "cmd=add,pk=8,string1=F,long1=66322,double1=1.5;"
                            "cmd=add,pk=9,string1=G,long1=100,double1=1.5;"
                            "cmd=add,pk=10,string1=H,long1=101,double1=1.5;"
                            "cmd=add,pk=11,string1=I,long1=119,double1=1.5;"
                            "cmd=add,pk=12,string1=J,long1=311,double1=1.5;"
                            "cmd=add,pk=13,string1=K,long1=302,double1=1.5;"
                            "cmd=add,pk=14,string1=M,long1=499,double1=1.5;";

        auto docs = DocumentCreator::CreateNormalDocuments(schema, docStrings);
        ASSERT_EQ(14u, docs.size());
        for (const auto& doc : docs) {
            updater.Update(doc);
        }
        auto jsonMap = updater.Dump();
        json::JsonMap expected;
        expected["baseline:long1"] = 400;
        EXPECT_EQ(ToJsonString(expected), ToJsonString(jsonMap));
    }

    {
        util::KeyValueMap parameters;
        parameters["attribute_name"] = "long1";
        parameters["period"] = "100";
        parameters["min_doc_count"] = "2";
        parameters["continuous_len"] = "2";

        TimeSeriesSegmentMetricsUpdater updater(nullptr);
        ASSERT_TRUE(updater.Init(schema, options, parameters));
        // no invalid baseline
        string docStrings = "cmd=add,pk=1,string1=A,long1=9999999,double1=1.1;"
                            "cmd=add,pk=2,string1=B,long1=9999999,double1=1.3;"
                            "cmd=add,pk=3,string1=C,long1=99999,double1=1.5;"
                            "cmd=add,pk=4,string1=D,long1=99988,double1=1.5;"
                            "cmd=add,pk=5,string1=L,long1=419,double1=1.5;"
                            "cmd=add,pk=7,string1=L,long1=99811,double1=1.5;"
                            "cmd=add,pk=6,string1=E,long1=88877,double1=1.5;"
                            "cmd=add,pk=8,string1=F,long1=66322,double1=1.5;"
                            "cmd=add,pk=9,string1=G,long1=100,double1=1.5;"
                            "cmd=add,pk=10,string1=H,long1=101,double1=1.5;"
                            "cmd=add,pk=11,string1=I,long1=119,double1=1.5;"
                            "cmd=add,pk=12,string1=J,long1=311,double1=1.5;"
                            "cmd=add,pk=13,string1=K,long1=302,double1=1.5;";

        auto docs = DocumentCreator::CreateNormalDocuments(schema, docStrings);
        ASSERT_EQ(13u, docs.size());
        for (const auto& doc : docs) {
            updater.Update(doc);
        }
        auto jsonMap = updater.Dump();
        json::JsonMap expected;
        expected["baseline:long1"] = 9999900;
        EXPECT_EQ(ToJsonString(expected), ToJsonString(jsonMap));
    }
}
}} // namespace indexlib::index
