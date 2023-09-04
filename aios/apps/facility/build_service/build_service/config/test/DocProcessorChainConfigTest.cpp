#include "build_service/config/DocProcessorChainConfig.h"

#include "build_service/test/unittest.h"

using namespace std;

namespace build_service { namespace config {

class DocProcessorChainConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    std::string readTestFileContent(const std::string& file) const;
};

void DocProcessorChainConfigTest::setUp() {}

void DocProcessorChainConfigTest::tearDown() {}

TEST_F(DocProcessorChainConfigTest, testSimpleProcess)
{
    string fileContent = readTestFileContent("processor_config.json");
    ASSERT_FALSE(fileContent.empty());
    DocProcessorChainConfig config;
    EXPECT_FALSE(config.useRawDocAsDoc);

    EXPECT_NO_THROW(FromJsonString(config, fileContent));

    EXPECT_EQ(size_t(2), config.clusterNames.size());
    EXPECT_EQ("simple_cluster1", config.clusterNames[0]);
    EXPECT_EQ("simple_cluster2", config.clusterNames[1]);
    EXPECT_EQ("chain1", config.chainName);

    EXPECT_TRUE(config.useRawDocAsDoc);

    EXPECT_EQ(size_t(1), config.moduleInfos.size());
    EXPECT_EQ("user_module_name", config.moduleInfos[0].moduleName);
    EXPECT_EQ("libuser_module.so", config.moduleInfos[0].modulePath);
    EXPECT_EQ(size_t(0), config.moduleInfos[0].parameters.size());

    EXPECT_EQ(size_t(2), config.processorInfos.size());

    EXPECT_EQ(string("UserDocProcessor"), config.processorInfos[0].className);
    EXPECT_EQ(string("user_module_name"), config.processorInfos[0].moduleName);
    EXPECT_EQ(size_t(2), config.processorInfos[0].parameters.size());
    EXPECT_EQ(string("value0"), config.processorInfos[0].parameters["key0"]);
    EXPECT_EQ(string("value1"), config.processorInfos[0].parameters["key1"]);

    EXPECT_EQ(string("TokenizeDocumentProcessor"), config.processorInfos[1].className);
    EXPECT_EQ(string(""), config.processorInfos[1].moduleName);
    EXPECT_EQ(size_t(0), config.processorInfos[1].parameters.size());

    EXPECT_EQ(size_t(1), config.subProcessorInfos.size());

    EXPECT_EQ(string("TokenizeDocumentProcessor"), config.subProcessorInfos[0].className);
    EXPECT_EQ(string(""), config.subProcessorInfos[0].moduleName);
    EXPECT_EQ(size_t(0), config.subProcessorInfos[0].parameters.size());
}

TEST_F(DocProcessorChainConfigTest, testInvalidFormat)
{
    DocProcessorChainConfig config;
    EXPECT_THROW(FromJsonString(config, "invalid json string"), autil::legacy::ParameterInvalidException);
}

TEST_F(DocProcessorChainConfigTest, testEmptySections)
{
    {
        string fileContent = readTestFileContent("processor_config_no_clusters.json");
        ASSERT_FALSE(fileContent.empty());
        DocProcessorChainConfig config;
        EXPECT_THROW(FromJsonString(config, fileContent), autil::legacy::NotJsonizableException);
    }
    {
        string fileContent = readTestFileContent("processor_config_no_main_chain.json");
        ASSERT_FALSE(fileContent.empty());
        DocProcessorChainConfig config;
        EXPECT_NO_THROW(FromJsonString(config, fileContent));
        EXPECT_TRUE(config.processorInfos.empty());
    }
    {
        string fileContent = readTestFileContent("processor_config_no_sub_chain.json");
        ASSERT_FALSE(fileContent.empty());
        DocProcessorChainConfig config;
        EXPECT_NO_THROW(FromJsonString(config, fileContent));
        EXPECT_TRUE(config.subProcessorInfos.empty());
    }
}

string DocProcessorChainConfigTest::readTestFileContent(const string& file) const
{
    string fileContent;
    string filePath = GET_TEST_DATA_PATH() + "doc_processor_config_test/" + file;
    fslib::util::FileUtil::readFile(filePath, fileContent);
    return fileContent;
}

}} // namespace build_service::config
