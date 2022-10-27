#include "indexlib/test/unittest.h"
#include "indexlib/config/module_info.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_BEGIN(config);

class ModuleInfoTest : public INDEXLIB_TESTBASE {
protected:
    static string readTestFileContent(const string& file) {
        string fileContent;
        string filePath = TEST_DATA_PATH"plugins/" + file;
        FileSystemWrapper::AtomicLoad(filePath, fileContent);
        return fileContent;
    }
};

TEST_F(ModuleInfoTest, testSimpleProcess) {
    std::string fileContent = readTestFileContent("module_config.json");
    ASSERT_FALSE(fileContent.empty());
    ModuleInfo moduleInfo;
    EXPECT_NO_THROW(FromJsonString(moduleInfo, fileContent));
    EXPECT_EQ(string("module_name1"), moduleInfo.moduleName);
    EXPECT_EQ(string("module_path1"), moduleInfo.modulePath);
    EXPECT_EQ(string("value1"), moduleInfo.parameters["key1"]);
}

TEST_F(ModuleInfoTest, testEmptySection) {
    {
        // module name can't be empty
        std::string fileContent = readTestFileContent(
                "module_config_no_module_name.json");
        ASSERT_FALSE(fileContent.empty());
        ModuleInfo moduleInfo;
        EXPECT_THROW(FromJsonString(moduleInfo, fileContent),
                     autil::legacy::NotJsonizableException);
    }
    {
        // module path can't be empty
        std::string fileContent = readTestFileContent(
                "module_config_no_module_path.json");
        ASSERT_FALSE(fileContent.empty());
        ModuleInfo moduleInfo;
        EXPECT_THROW(FromJsonString(moduleInfo, fileContent),
                     autil::legacy::NotJsonizableException);
    }
    {
        // parameters can be empty
        std::string fileContent = readTestFileContent(
                "module_config_no_parameters.json");
        ASSERT_FALSE(fileContent.empty());
        ModuleInfo moduleInfo;
        EXPECT_NO_THROW(FromJsonString(moduleInfo, fileContent));
        EXPECT_TRUE(moduleInfo.parameters.empty());
    }
}

IE_NAMESPACE_END(config);
