#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/test/unittest.h"

using namespace std;

namespace build_service { namespace config {

class ProcessorInfoTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    std::string readTestFileContent(const std::string& file);
};

void ProcessorInfoTest::setUp() {}

void ProcessorInfoTest::tearDown() {}

TEST_F(ProcessorInfoTest, testSimpleProcess)
{
    string fileContent = readTestFileContent("processor_info.json");
    ASSERT_FALSE(fileContent.empty());
    ProcessorInfo processorInfo;
    EXPECT_NO_THROW(FromJsonString(processorInfo, fileContent));
    EXPECT_EQ(string("class1"), processorInfo.className);
    EXPECT_EQ(string("module1"), processorInfo.moduleName);
    EXPECT_EQ(string("value0"), processorInfo.parameters["key0"]);
    EXPECT_EQ(string("value1"), processorInfo.parameters["key1"]);
}

TEST_F(ProcessorInfoTest, testEmptySection)
{
    {
        // class name can't be empty
        string fileContent = readTestFileContent("processor_info_no_class_name.json");
        ASSERT_FALSE(fileContent.empty());
        ProcessorInfo processorInfo;
        EXPECT_THROW(FromJsonString(processorInfo, fileContent), autil::legacy::NotJsonizableException);
    }
    {
        // module name can be empty (for build in plugins)
        string fileContent = readTestFileContent("processor_info_no_module_name.json");
        ASSERT_FALSE(fileContent.empty());
        ProcessorInfo processorInfo;
        EXPECT_NO_THROW(FromJsonString(processorInfo, fileContent));
        EXPECT_EQ(string(), processorInfo.moduleName);
    }
    {
        // parameters can be empty
        string fileContent = readTestFileContent("processor_info_no_parameters.json");
        ASSERT_FALSE(fileContent.empty());
        ProcessorInfo processorInfo;
        EXPECT_NO_THROW(FromJsonString(processorInfo, fileContent));
        EXPECT_TRUE(processorInfo.parameters.empty());
    }
}

string ProcessorInfoTest::readTestFileContent(const string& file)
{
    string fileContent;
    string filePath = GET_TEST_DATA_PATH() + "doc_processor_config_test/" + file;
    fslib::util::FileUtil::readFile(filePath, fileContent);
    return fileContent;
}

}} // namespace build_service::config
