#include "build_service/config/ProcessorConfigurator.h"

#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace testing;

using namespace build_service::proto;

namespace build_service { namespace config {

class ProcessorConfiguratorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ProcessorConfiguratorTest::setUp() {}

void ProcessorConfiguratorTest::tearDown() {}

TEST_F(ProcessorConfiguratorTest, testAutoConf)
{
    {
        ProcessorConfigurator configurator;
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));

        PartitionId pid = ProtoCreator::createPartitionId(ROLE_PROCESSOR, BUILD_STEP_FULL, 0, 65535, "simple", 0,
                                                          "cluster0", "", "1");

        DataDescription ds;
        ds["need_field_filter"] = "false";
        ds["reader_config"] = "";
        ds["src"] = "swift";
        ds["swift_root"] = "zfs://root";
        ds["topic_name"] = "raw_doc";
        ds["type"] = "swift";
        ds["full_processor_count"] = "64";

        uint64_t maxBufferSize = 0;
        string configName =
            pid.step() == BUILD_STEP_FULL ? FULL_SWIFT_BROKER_TOPIC_CONFIG : INC_SWIFT_BROKER_TOPIC_CONFIG;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster0",
                                                             ds, maxBufferSize));
        EXPECT_EQ(
            (uint64_t)(8192ll * 1024 * 1024 * SwiftTopicConfig::DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR / 64),
            maxBufferSize);

        // test buffer size calculation is ineffective when topic mode is not memory_prefer
        maxBufferSize = 0;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster1",
                                                             ds, maxBufferSize));
        EXPECT_EQ(ProcessorConfigurator::DEFAULT_WRITER_BUFFER_SIZE, maxBufferSize);
    }
    {
        ProcessorConfigurator configurator;
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));

        PartitionId pid = ProtoCreator::createPartitionId(ROLE_PROCESSOR, BUILD_STEP_FULL, 0, 65535, "simple", 0,
                                                          "cluster3", "", "1");

        DataDescription ds;
        ds["need_field_filter"] = "false";
        ds["reader_config"] = "";
        ds["src"] = "swift";
        ds["swift_root"] = "zfs://root";
        ds["topic_name"] = "raw_doc";
        ds["type"] = "swift";
        ds["full_processor_count"] = "64";

        uint64_t maxBufferSize = 0;
        string configName =
            pid.step() == BUILD_STEP_FULL ? FULL_SWIFT_BROKER_TOPIC_CONFIG : INC_SWIFT_BROKER_TOPIC_CONFIG;

        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster3",
                                                             ds, maxBufferSize));
        EXPECT_EQ((uint64_t)(1024ll / 2 * 2 * 3 * 1024 * 1024 * 1.9 / 64), maxBufferSize);

        // test buffer size calculation is ineffective when topic mode is not memory_prefer
        maxBufferSize = 0;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster1",
                                                             ds, maxBufferSize));
        EXPECT_EQ(ProcessorConfigurator::DEFAULT_WRITER_BUFFER_SIZE, maxBufferSize);
    }
    {
        ProcessorConfigurator configurator;
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));

        PartitionId pid =
            ProtoCreator::createPartitionId(ROLE_PROCESSOR, BUILD_STEP_INC, 0, 65535, "simple", 0, "cluster3", "", "1");

        DataDescription ds;
        ds["need_field_filter"] = "false";
        ds["reader_config"] = "";
        ds["src"] = "swift";
        ds["swift_root"] = "zfs://root";
        ds["topic_name"] = "raw_doc";
        ds["type"] = "swift";
        ds["full_processor_count"] = "64";

        uint64_t maxBufferSize = 0;
        string configName =
            pid.step() == BUILD_STEP_FULL ? FULL_SWIFT_BROKER_TOPIC_CONFIG : INC_SWIFT_BROKER_TOPIC_CONFIG;

        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster3",
                                                             ds, maxBufferSize));
        EXPECT_EQ(ProcessorConfigurator::DEFAULT_WRITER_BUFFER_SIZE, maxBufferSize);

        // test buffer size calculation is ineffective when topic mode is not memory_prefer
        maxBufferSize = 0;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster4",
                                                             ds, maxBufferSize));
        EXPECT_EQ(5678u, maxBufferSize);
    }
}

TEST_F(ProcessorConfiguratorTest, testSrcNodeConfigValid)
{
    {
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "/config_src_test/"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", "processor_1"));
        vector<string> clusterNames = {"update2add"};
        ASSERT_TRUE(reader->validateConfig(clusterNames));
    }

    {
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "/config_src_test/"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", "processor_2"));
        vector<string> clusterNames = {"update2add"};
        ASSERT_FALSE(reader->validateConfig(clusterNames));
    }
}

TEST_F(ProcessorConfiguratorTest, testCustomize)
{
    {
        ProcessorConfigurator configurator;
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));

        PartitionId pid = ProtoCreator::createPartitionId(ROLE_PROCESSOR, BUILD_STEP_FULL, 0, 65535, "simple", 0,
                                                          "cluster2", "", "1");

        DataDescription ds;
        ds["need_field_filter"] = "false";
        ds["reader_config"] = "";
        ds["src"] = "swift";
        ds["swift_root"] = "zfs://root";
        ds["topic_name"] = "raw_doc";
        ds["type"] = "swift";
        ds["full_processor_count"] = "64";

        uint64_t maxBufferSize = 0;
        string configName =
            pid.step() == BUILD_STEP_FULL ? FULL_SWIFT_BROKER_TOPIC_CONFIG : INC_SWIFT_BROKER_TOPIC_CONFIG;

        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster2",
                                                             ds, maxBufferSize));
        EXPECT_EQ(1234u, maxBufferSize);

        // test buffer size calculation is ineffective when topic mode is not memory_prefer
        maxBufferSize = 0;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster1",
                                                             ds, maxBufferSize));
        EXPECT_EQ(ProcessorConfigurator::DEFAULT_WRITER_BUFFER_SIZE, maxBufferSize);
    }
    {
        ProcessorConfigurator configurator;
        ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));

        PartitionId pid = ProtoCreator::createPartitionId(ROLE_PROCESSOR, BUILD_STEP_FULL, 0, 65535, "simple", 0,
                                                          "cluster4", "", "1");

        DataDescription ds;
        ds["need_field_filter"] = "false";
        ds["reader_config"] = "";
        ds["src"] = "swift";
        ds["swift_root"] = "zfs://root";
        ds["topic_name"] = "raw_doc";
        ds["type"] = "swift";

        uint64_t maxBufferSize = 0;
        string configName =
            pid.step() == BUILD_STEP_FULL ? FULL_SWIFT_BROKER_TOPIC_CONFIG : INC_SWIFT_BROKER_TOPIC_CONFIG;
        ASSERT_TRUE(configurator.getSwiftWriterMaxBufferSize(reader, pid.buildid().datatable(), configName, "cluster4",
                                                             ds, maxBufferSize));
        EXPECT_EQ(5678u, maxBufferSize);
    }
}

TEST_F(ProcessorConfiguratorTest, testGetSchemaUpdatableClusterConfigVec)
{
    ProcessorConfigurator configurator;
    ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));

    DataDescription ds;
    ds["need_field_filter"] = "false";
    ds["reader_config"] = "";
    ds["src"] = "swift";
    ds["swift_root"] = "zfs://root";
    ds["topic_name"] = "raw_doc";
    ds["type"] = "swift";
    ds["full_processor_count"] = "64";

    {
        SchemaUpdatableClusterConfigVec configVec;
        bool isExist;
        ASSERT_TRUE(configurator.getSchemaUpdatableClusterConfigVec(resourceReader, "simple3", configVec, isExist));
        ASSERT_TRUE(isExist);
        ASSERT_EQ(1, configVec.size());
        ASSERT_EQ(3, configVec[0].partitionCount);
        ASSERT_EQ("cluster1", configVec[0].clusterName);
    }
    // test empty cluster name
    {
        SchemaUpdatableClusterConfigVec configVec;
        bool isExist;
        ASSERT_FALSE(configurator.getSchemaUpdatableClusterConfigVec(resourceReader, "simple4", configVec, isExist));
        ASSERT_TRUE(isExist);
        ASSERT_EQ(1, configVec.size());
        ASSERT_EQ(3, configVec[0].partitionCount);
        ASSERT_EQ("", configVec[0].clusterName);
    }
    // test invalid cluster name
    {
        SchemaUpdatableClusterConfigVec configVec;
        bool isExist;
        ASSERT_FALSE(configurator.getSchemaUpdatableClusterConfigVec(resourceReader, "simple5", configVec, isExist));
        ASSERT_TRUE(isExist);
        ASSERT_EQ(1, configVec.size());
        ASSERT_EQ(3, configVec[0].partitionCount);
        ASSERT_EQ("not-exist-cluster", configVec[0].clusterName);
    }
}

TEST_F(ProcessorConfiguratorTest, testGetSchemaUpdatableClusterConfig)
{
    ProcessorConfigurator configurator;
    ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));

    DataDescription ds;
    ds["need_field_filter"] = "false";
    ds["reader_config"] = "";
    ds["src"] = "swift";
    ds["swift_root"] = "zfs://root";
    ds["topic_name"] = "raw_doc";
    ds["type"] = "swift";
    ds["full_processor_count"] = "64";

    {
        // single cluster
        SchemaUpdatableClusterConfig config;
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "simple", ""));
        ASSERT_TRUE(configurator.getSchemaUpdatableClusterConfig(reader, "simple", "cluster0", config));
        ASSERT_EQ(2, config.partitionCount);
        ASSERT_EQ("cluster0", config.clusterName);
    }

    {
        // multi cluster
        SchemaUpdatableClusterConfig config;
        ProcessorConfigReaderPtr reader(new ProcessorConfigReader(resourceReader, "multi_cluster", ""));
        ASSERT_TRUE(configurator.getSchemaUpdatableClusterConfig(reader, "multi_cluster", "cluster3", config));
        ASSERT_EQ(3, config.partitionCount);
        ASSERT_EQ("cluster3", config.clusterName);

        ASSERT_FALSE(configurator.getSchemaUpdatableClusterConfig(reader, "multi_cluster", "cluster1", config));
    }
}

TEST_F(ProcessorConfiguratorTest, testGetSchemaUpdatableClusters)
{
    ProcessorConfigurator configurator;
    ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));

    DataDescription ds;
    ds["need_field_filter"] = "false";
    ds["reader_config"] = "";
    ds["src"] = "swift";
    ds["swift_root"] = "zfs://root";
    ds["topic_name"] = "raw_doc";
    ds["type"] = "swift";
    ds["full_processor_count"] = "64";

    {
        // multi cluster
        vector<string> clusterNames;
        ASSERT_TRUE(configurator.getSchemaUpdatableClusters(resourceReader, "multi_cluster", clusterNames));
        ASSERT_EQ(1, clusterNames.size());
        ASSERT_EQ("cluster3", clusterNames[0]);
    }
}

TEST_F(ProcessorConfiguratorTest, testGetAllClusters)
{
    ProcessorConfigurator configurator;
    ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));

    DataDescription ds;
    ds["need_field_filter"] = "false";
    ds["reader_config"] = "";
    ds["src"] = "swift";
    ds["swift_root"] = "zfs://root";
    ds["topic_name"] = "raw_doc";
    ds["type"] = "swift";
    ds["full_processor_count"] = "64";

    {
        // multi cluster
        vector<string> clusterNames;
        ASSERT_TRUE(configurator.getAllClusters(resourceReader, "multi_cluster", clusterNames));
        ASSERT_EQ(2, clusterNames.size());
        ASSERT_EQ("cluster1", clusterNames[0]);
        ASSERT_EQ("cluster3", clusterNames[1]);
    }
}

TEST_F(ProcessorConfiguratorTest, testGetSchemaNotUpdatableClusters)
{
    ProcessorConfigurator configurator;
    ResourceReaderPtr resourceReader(new ResourceReader(GET_TEST_DATA_PATH() + "admin_test/processorConfig"));

    DataDescription ds;
    ds["need_field_filter"] = "false";
    ds["reader_config"] = "";
    ds["src"] = "swift";
    ds["swift_root"] = "zfs://root";
    ds["topic_name"] = "raw_doc";
    ds["type"] = "swift";
    ds["full_processor_count"] = "64";

    {
        // multi cluster
        vector<string> clusterNames;
        ASSERT_TRUE(configurator.getSchemaNotUpdatableClusters(resourceReader, "multi_cluster", clusterNames));
        ASSERT_EQ(1, clusterNames.size());
        ASSERT_EQ("cluster1", clusterNames[0]);
    }
}

}} // namespace build_service::config
