
#include "build_service_tasks/extract_doc/ExtractDocTask.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/io/FileOutput.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/reader/StandardRawDocumentParser.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service_tasks/extract_doc/RawDocumentOutput.h"
#include "build_service_tasks/extract_doc/test/FakeOutputCreator.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/test/partition_state_machine.h"

using namespace std;
using namespace testing;
using namespace build_service::io;
using namespace build_service::config;
using namespace autil::legacy;
using namespace build_service_tasks;

using namespace indexlib::document;
namespace build_service { namespace task_base {

class ExtractDocTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();
    void prepareIndexAndConfig();
    void checkDocs(const char* fieldName, std::initializer_list<const char*> expectedList,
                   const vector<DefaultRawDocument>& docs)
    {
        for (size_t i = 0; i < docs.size(); ++i) {
            auto expected = string(*(expectedList.begin() + i));
            EXPECT_EQ(expected, docs[i].getField(string(fieldName))) << i << "th doc failed";
        }
    }
    vector<DefaultRawDocument> parseDocs(const vector<swift::common::MessageInfo>& msgs)
    {
        vector<DefaultRawDocument> rawDocs;
        for (const auto& msg : msgs) {
            DefaultRawDocument rawDoc;
            EXPECT_TRUE(_parser->parse(msg.data, rawDoc));
            EXPECT_EQ(ADD_DOC, rawDoc.getDocOperateType());
            rawDocs.push_back(rawDoc);
        }
        return rawDocs;
    }
    void CheckMetricsValue(indexlib::util::MetricProviderPtr provider, const string& name, const string& unit,
                           kmonitor::MetricType type, double value);

protected:
    config::BuildServiceConfig _serviceConfig;
    proto::PartitionId _pid;
    reader::RawDocumentParserPtr _parser;
    std::string _taskParam;
};

class FakeExtractDocTask : public ExtractDocTask
{
public:
    void setServiceConfig(const config::BuildServiceConfig& serviceConfig) { _serviceConfig = serviceConfig; }

private:
    bool getServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig) override
    {
        serviceConfig = _serviceConfig;
        return true;
    }
    config::BuildServiceConfig _serviceConfig;
};

void ExtractDocTaskTest::setUp()
{
    string dsStr = "{"
                   "\"src\" : \"swift\","
                   "\"type\" : \"swift\","
                   "\"swift_root\" : \"zfs://xxx-swift/xxx-swift-service\""
                   "}";
    KeyValueMap kvMap;
    kvMap[config::DATA_DESCRIPTION_KEY] = dsStr;
    reader::ParserCreator creator;
    _parser.reset(creator.create(kvMap));
}

void ExtractDocTaskTest::tearDown() {}

void ExtractDocTaskTest::prepareIndexAndConfig()
{
    proto::Range range;
    range.set_from(0);
    range.set_to(65535);
    _pid = proto::PartitionId();
    _pid.set_role(proto::ROLE_TASK);
    _pid.set_step(proto::BUILD_STEP_INC);
    _pid.set_taskid("123_cluster1_extract_doc");
    *_pid.mutable_range() = range;
    *_pid.add_clusternames() = "cluster1";
    string indexRoot = GET_TEMP_DATA_PATH();
    string indexPath = util::IndexPathConstructor::constructIndexPath(indexRoot, _pid);
    _serviceConfig._indexRoot = indexRoot;

    _taskParam = R"(
{
    "query" : "[{\"type\" : \"TERM\",\"index_info\" : { \"index\" : \"service_id\", \"term\" : \"0\" } }]",
    "extract_doc_count" : "2",
    "output_param" : "{
          \"output_name\" : \"output1\",
          \"inner_output_type\" : \"SWIFT\",
          \"zk_root\" : \"test\",
          \"topic_name\" : \"123\",
          \"hash_field\" : \"pk\",
          \"hash_mode\" : \"HASH\"
    }"
})";

    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    auto schema = resourceReader->getSchema("cluster1");
    indexlib::config::IndexPartitionOptions options;
    indexlib::test::PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, indexPath));
    ASSERT_TRUE(psm.Transfer(indexlib::test::BUILD_FULL,
                             "cmd=add,pk_field=0,service_id=0,price=1,ops_summary=aaaaaa;"
                             "cmd=add,pk_field=1,service_id=1,price=1,ops_summary=bbbbbb;"
                             "cmd=add,pk_field=2,service_id=0,price=1,ops_summary=cccccc;"
                             "cmd=add,pk_field=3,service_id=1,price=1,ops_summary=dddddd;"
                             "cmd=add,pk_field=4,service_id=0,price=1,ops_summary=eeeeee;",
                             //"cmd=add,pk_field=5,service_id=0,price=2,ops_summary=ffffff;",
                             "", ""));
    ASSERT_TRUE(psm.Transfer(indexlib::test::QUERY, "", "service_id:0", "docid=0;docid=2;docid=4;"));
}

void ExtractDocTaskTest::CheckMetricsValue(indexlib::util::MetricProviderPtr provider, const string& name,
                                           const string& unit, kmonitor::MetricType type, double value)
{
    indexlib::util::MetricPtr metric = provider->DeclareMetric(name, type);
    ASSERT_TRUE(metric != nullptr);
    // NOTE: only support STATUS type
    EXPECT_DOUBLE_EQ(value, metric->_value) << "metric [" << name << "]not equal";
}

TEST_F(ExtractDocTaskTest, testSimple)
{
    prepareIndexAndConfig();
    FakeExtractDocTask task;
    task.setServiceConfig(_serviceConfig);
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    initParam.resourceReader = resourceReader;
    indexlib::util::MetricProviderPtr metricProvider(new indexlib::util::MetricProvider);
    initParam.metricProvider = metricProvider;
    initParam.clusterName = "cluster1";
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 2;
    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_SNAPSHOT_VERSION, "1");
    target.addTargetDescription(BS_TASK_CONFIG_FILE_PATH,
                                GET_TEST_DATA_PATH() + "/extract_doc_test/config/clusters/extract_doc_task.json");
    target.addTargetDescription("task_param", _taskParam);
    TaskOutputConfig outputConfig;
    KeyValueMap params;
    FakeRawDocumentOutputCreatorPtr output1(new FakeRawDocumentOutputCreator());
    output1->init(outputConfig);
    FakeRawDocumentOutputCreatorPtr output2(new FakeRawDocumentOutputCreator());
    output2->init(outputConfig);

    initParam.outputCreators["output1"] = output1;
    initParam.outputCreators["output2"] = output2;
    ASSERT_TRUE(task.init(initParam));

    // CheckResource
    ASSERT_FALSE(task.isDone(target));
    ASSERT_NO_THROW(task.handleTarget(target));
    RawDocumentOutputPtr rawDocOutput = dynamic_pointer_cast<RawDocumentOutput>(output1->getOutput());
    ASSERT_TRUE(rawDocOutput);
    FakeSwiftOutputPtr innerOutput = dynamic_pointer_cast<FakeSwiftOutput>(rawDocOutput->getInnerOutput());
    ASSERT_TRUE(innerOutput);

    auto msgs = innerOutput->getOutputMessages();
    ASSERT_EQ(2u, msgs.size());
    auto docs = parseDocs(msgs);
    ASSERT_EQ(2u, docs.size());
    CheckMetricsValue(metricProvider, "extractDocCount", "count", kmonitor::STATUS, 2);

    checkDocs("service_id", {"0", "0"}, docs);
    checkDocs("pk_field", {"0", "2"}, docs);
    checkDocs("price", {"1", "1"}, docs);
    checkDocs("ops_summary", {"aaaaaa", "cccccc"}, docs);
    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
    EXPECT_EQ(msgs.size(), innerOutput->getOutputMessages().size());
}

TEST_F(ExtractDocTaskTest, testFileOutput)
{
    string outputPath = GET_TEMP_DATA_PATH() + "/output.data/";

    string taskParam = R"(
{
    "query" : "[{\"type\" : \"TERM\",\"index_info\" : { \"index\" : \"service_id\", \"term\" : \"0\" } }]",
    "extract_doc_count" : "2",
    "output_param" : "{
        \"output_name\" : \"output3\",
        \"inner_output_type\" : \"FILE\",
        \"hash_field\" : \"pk\",
        \"hash_mode\" : \"HASH\",
        \"file_path\" : \"file_path_replaced\"
    }"
})";
    const string FILE_PATH_REPLACED = "file_path_replaced";
    taskParam.replace(taskParam.find(FILE_PATH_REPLACED), FILE_PATH_REPLACED.length(), outputPath);

    prepareIndexAndConfig();
    FakeExtractDocTask task;
    task.setServiceConfig(_serviceConfig);
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    initParam.resourceReader = resourceReader;
    indexlib::util::MetricProviderPtr metricProvider(new indexlib::util::MetricProvider);
    initParam.metricProvider = metricProvider;
    initParam.clusterName = "cluster1";
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 2;
    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_SNAPSHOT_VERSION, "1");
    target.addTargetDescription(BS_TASK_CONFIG_FILE_PATH,
                                GET_TEST_DATA_PATH() + "/extract_doc_test/config/clusters/extract_doc_task.json");
    target.addTargetDescription("task_param", taskParam);
    TaskOutputConfig outputConfig;
    KeyValueMap params;

    FakeRawDocumentOutputCreatorPtr output1(new FakeRawDocumentOutputCreator());
    FakeRawDocumentOutputCreatorPtr output2(new FakeRawDocumentOutputCreator());
    FakeRawDocumentOutputCreatorPtr output3(new FakeRawDocumentOutputCreator());
    output1->init(outputConfig);
    output2->init(outputConfig);
    output3->init(outputConfig);
    initParam.outputCreators["output1"] = output1;
    initParam.outputCreators["output2"] = output2;
    initParam.outputCreators["output3"] = output3;
    ASSERT_TRUE(task.init(initParam));

    // CheckResource
    ASSERT_FALSE(task.isDone(target));
    ASSERT_TRUE(task.handleTarget(target));
    RawDocumentOutputPtr rawDocOutput = dynamic_pointer_cast<RawDocumentOutput>(output3->getOutput());
    ASSERT_TRUE(rawDocOutput);
    FileOutputPtr innerOutput = dynamic_pointer_cast<FileOutput>(rawDocOutput->getInnerOutput());
    ASSERT_TRUE(innerOutput);
    CheckMetricsValue(metricProvider, "extractDocCount", "count", kmonitor::STATUS, 2);

    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));

    string content;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(outputPath, content));
    ASSERT_EQ(132u, content.size());
}

TEST_F(ExtractDocTaskTest, testMultiFileOutput)
{
    string outputPath = GET_TEMP_DATA_PATH() + "/output_dir/";
    string taskParam = R"(
{
    "query" : "[{\"type\" : \"TERM\",\"index_info\" : { \"index\" : \"service_id\", \"term\" : \"0\" } }]",
    "extract_doc_count" : "2",
    "output_param" : "{
        \"output_name\" : \"output4\",
        \"inner_output_type\" : \"MULTI_FILE\",
        \"hash_field\" : \"pk\",
        \"hash_mode\" : \"HASH\",
        \"dest_directory\" : \"directory_path_replaced\",
        \"file_prefix\" : \"output_data\"
    }"
})";
    const string DIRECTORY_PATH_REPLACED = "directory_path_replaced";
    taskParam.replace(taskParam.find(DIRECTORY_PATH_REPLACED), DIRECTORY_PATH_REPLACED.length(), outputPath);
    fslib::fs::FileSystem::mkDir(outputPath);

    prepareIndexAndConfig();
    FakeExtractDocTask task;
    task.setServiceConfig(_serviceConfig);
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    initParam.resourceReader = resourceReader;
    indexlib::util::MetricProviderPtr metricProvider(new indexlib::util::MetricProvider);
    initParam.metricProvider = metricProvider;
    initParam.clusterName = "cluster1";
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 2;
    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_SNAPSHOT_VERSION, "1");
    target.addTargetDescription(BS_TASK_CONFIG_FILE_PATH,
                                GET_TEST_DATA_PATH() + "/extract_doc_test/config/clusters/extract_doc_task.json");
    target.addTargetDescription("task_param", taskParam);
    TaskOutputConfig outputConfig;
    KeyValueMap params;

    FakeRawDocumentOutputCreatorPtr output4(new FakeRawDocumentOutputCreator());
    output4->init(outputConfig);
    initParam.outputCreators["output4"] = output4;
    ASSERT_TRUE(task.init(initParam));

    // CheckResource
    ASSERT_FALSE(task.isDone(target));
    ASSERT_TRUE(task.handleTarget(target));
    RawDocumentOutputPtr rawDocOutput = dynamic_pointer_cast<RawDocumentOutput>(output4->getOutput());
    ASSERT_TRUE(rawDocOutput);
    MultiFileOutputPtr innerOutput = dynamic_pointer_cast<MultiFileOutput>(rawDocOutput->getInnerOutput());
    ASSERT_TRUE(innerOutput);
    CheckMetricsValue(metricProvider, "extractDocCount", "count", kmonitor::STATUS, 2);

    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
}

TEST_F(ExtractDocTaskTest, testWithCheckpoint)
{
    prepareIndexAndConfig();
    FakeExtractDocTask task;
    task.setServiceConfig(_serviceConfig);

    TaskOutputConfig outputConfig;
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    initParam.resourceReader = resourceReader;
    initParam.clusterName = "cluster1";
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 2;
    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_SNAPSHOT_VERSION, "1");
    target.addTargetDescription(BS_TASK_CONFIG_FILE_PATH,
                                GET_TEST_DATA_PATH() + "/extract_doc_test/config/clusters/extract_doc_task.json");
    target.addTargetDescription("task_param", _taskParam);
    ExtractDocTask::Checkpoint checkpoint;
    checkpoint.docCount = 1;
    checkpoint.offset = 1;
    target.addTargetDescription(BS_EXTRACT_DOC_CHECKPOINT, checkpoint.toString());

    KeyValueMap params;
    FakeRawDocumentOutputCreatorPtr output1(new FakeRawDocumentOutputCreator());
    output1->init(outputConfig);
    FakeRawDocumentOutputCreatorPtr output2(new FakeRawDocumentOutputCreator());
    output2->init(outputConfig);

    initParam.outputCreators["output1"] = output1;
    initParam.outputCreators["output2"] = output2;
    ASSERT_TRUE(task.init(initParam));

    ASSERT_FALSE(task.isDone(target));
    ASSERT_TRUE(task.handleTarget(target));
    RawDocumentOutputPtr rawDocOutput = dynamic_pointer_cast<RawDocumentOutput>(output1->getOutput());
    ASSERT_TRUE(rawDocOutput);
    FakeSwiftOutputPtr innerOutput = dynamic_pointer_cast<FakeSwiftOutput>(rawDocOutput->getInnerOutput());
    ASSERT_TRUE(innerOutput);
    auto msgs = innerOutput->getOutputMessages();
    ASSERT_EQ(1u, msgs.size());
    auto docs = parseDocs(msgs);
    ASSERT_EQ(1u, docs.size());

    checkDocs("service_id", {"0"}, docs);
    checkDocs("pk_field", {"2"}, docs);
    checkDocs("price", {"1"}, docs);
    checkDocs("ops_summary", {"cccccc"}, docs);
    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
}

TEST_F(ExtractDocTaskTest, testAppendFields)
{
    string taskParam = R"(
{
    "query" : "[{\"type\" : \"TERM\",\"index_info\" : { \"index\" : \"service_id\", \"term\" : \"0\" } }]",
    "extract_doc_count" : "2",
    "output_param" : "{
        \"output_name\" : \"output1\",
        \"inner_output_type\" : \"SWIFT\",
        \"zk_root\" : \"test\",
        \"topic_name\" : \"123\",
        \"hash_field\" : \"pk\",
        \"hash_mode\" : \"HASH\"
    }",
    "append_fields" : "{\"append\" : \"aa\"}"
})";
    prepareIndexAndConfig();
    FakeExtractDocTask task;
    task.setServiceConfig(_serviceConfig);
    Task::TaskInitParam initParam;
    config::ResourceReaderPtr resourceReader(
        new config::ResourceReader(GET_TEST_DATA_PATH() + "/extract_doc_test/config/"));
    initParam.resourceReader = resourceReader;
    indexlib::util::MetricProviderPtr metricProvider(new indexlib::util::MetricProvider);
    initParam.metricProvider = metricProvider;
    initParam.clusterName = "cluster1";
    initParam.pid = _pid;
    initParam.instanceInfo.instanceId = 1;
    initParam.instanceInfo.instanceCount = 2;
    config::TaskTarget target("do_task");
    target.addTargetDescription(BS_SNAPSHOT_VERSION, "1");
    target.addTargetDescription(BS_TASK_CONFIG_FILE_PATH,
                                GET_TEST_DATA_PATH() + "/extract_doc_test/config/clusters/extract_doc_task.json");
    target.addTargetDescription("task_param", taskParam);
    TaskOutputConfig outputConfig;
    KeyValueMap params;
    FakeRawDocumentOutputCreatorPtr output1(new FakeRawDocumentOutputCreator());
    output1->init(outputConfig);
    FakeRawDocumentOutputCreatorPtr output2(new FakeRawDocumentOutputCreator());
    output2->init(outputConfig);

    initParam.outputCreators["output1"] = output1;
    initParam.outputCreators["output2"] = output2;
    ASSERT_TRUE(task.init(initParam));

    // CheckResource
    ASSERT_FALSE(task.isDone(target));
    ASSERT_NO_THROW(task.handleTarget(target));
    RawDocumentOutputPtr rawDocOutput = dynamic_pointer_cast<RawDocumentOutput>(output1->getOutput());
    ASSERT_TRUE(rawDocOutput);
    FakeSwiftOutputPtr innerOutput = dynamic_pointer_cast<FakeSwiftOutput>(rawDocOutput->getInnerOutput());
    ASSERT_TRUE(innerOutput);
    auto msgs = innerOutput->getOutputMessages();
    ASSERT_EQ(2u, msgs.size());
    auto docs = parseDocs(msgs);
    ASSERT_EQ(2u, docs.size());
    CheckMetricsValue(metricProvider, "extractDocCount", "count", kmonitor::STATUS, 2);

    checkDocs("service_id", {"0", "0"}, docs);
    checkDocs("pk_field", {"0", "2"}, docs);
    checkDocs("price", {"1", "1"}, docs);
    checkDocs("ops_summary", {"aaaaaa", "cccccc"}, docs);
    // test append fields
    checkDocs("append", {"aa", "aa"}, docs);
    ASSERT_TRUE(task.isDone(target));
    // Will not redo work
    ASSERT_NO_THROW(task.handleTarget(target));
    ASSERT_TRUE(task.isDone(target));
    EXPECT_EQ(msgs.size(), innerOutput->getOutputMessages().size());
}

}} // namespace build_service::task_base
