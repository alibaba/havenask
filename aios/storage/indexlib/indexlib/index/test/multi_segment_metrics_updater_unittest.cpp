#include "indexlib/index/test/multi_segment_metrics_updater_unittest.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index/segment_metrics_updater/multi_segment_metrics_updater.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::plugin;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiSegmentMetricsUpdaterTest);

MultiSegmentMetricsUpdaterTest::MultiSegmentMetricsUpdaterTest() {}

MultiSegmentMetricsUpdaterTest::~MultiSegmentMetricsUpdaterTest() {}

void MultiSegmentMetricsUpdaterTest::CaseSetUp() {}

void MultiSegmentMetricsUpdaterTest::CaseTearDown() {}

void MultiSegmentMetricsUpdaterTest::TestSimpleProcess()
{
    string field = "pk:uint64:pk;string1:string;long1:uint32;double1:double;";
    string index = "pk:primarykey64:pk;";
    string attr = "string1;long1;double1";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, "");
    auto attrSchema = schema->GetAttributeSchema();
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    assert(attrSchema);
    {
        PluginManagerPtr pluginManager(new PluginManager(""));
        vector<ModuleClassConfig> updaterConfig;
        ModuleClassConfig config1;
        config1.className = "lifecycle";
        config1.parameters["attribute_name"] = "long1";
        config1.parameters["default_lifecycle_tag"] = "hot";
        updaterConfig.push_back(config1);

        ModuleClassConfig config2;
        config2.className = "max_min";
        config2.parameters["attribute_name"] = "double1";
        updaterConfig.push_back(config2);

        MultiSegmentMetricsUpdater updater(nullptr);
        ASSERT_NO_THROW(updater.Init(schema, options, updaterConfig, pluginManager));
        string docStrings = "cmd=add,pk=1,string1=A,long1=100,double1=1.1;"
                            "cmd=add,pk=2,string1=B,long1=10,double1=1.3;"
                            "cmd=add,pk=3,string1=C,long1=55,double1=1.5;";
        auto docs = DocumentCreator::CreateNormalDocuments(schema, docStrings);
        ASSERT_EQ(3u, docs.size());
        for (const auto& doc : docs) {
            updater.Update(doc);
        }
        auto jsonMap = updater.Dump();
        json::JsonMap expected;
        expected["checked_doc_count"] = 3;
        json::JsonMap attrValues;
        attrValues["max"] = 100u;
        attrValues["min"] = 10u;
        expected["attribute:long1"] = attrValues;
        json::JsonMap attrValues2;
        attrValues2["max"] = (double)1.5;
        attrValues2["min"] = (double)1.1;
        expected["attribute:double1"] = attrValues2;
        expected[LIFECYCLE] = string("hot");
        EXPECT_EQ(ToJsonString(expected), ToJsonString(jsonMap));
    }
}
}} // namespace indexlib::index
