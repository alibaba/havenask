#include "build_service/test/unittest.h"
#include "build_service/config/ConfigParser.h"
#include "build_service/util/FileUtil.h"
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::util;

namespace build_service {
namespace config {

class ConfigParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void testParser(const string& configRoot, const string &exepctResultFile,
                    const string &testFile) const;
    void replaceFile(const string& filePath, const string& strToReplace,
                     const string& replaceStr);

private:
    string _configRoot;
};

void ConfigParserTest::setUp() {
    _configRoot = TEST_DATA_PATH"config_with_imports/";
}

void ConfigParserTest::tearDown() {
}

void ConfigParserTest::testParser(const string& configRoot,
                                  const string &expectResultFile, const string &testFile) const {
    JsonMap jsonMap;
    ConfigParser parser(configRoot);
    ASSERT_TRUE(parser.parse(testFile, jsonMap));
    string actualResult = ToJsonString(jsonMap);
    string expectResult;
    ASSERT_TRUE(FileUtil::readFile(configRoot + expectResultFile, expectResult));
    FromJsonString(jsonMap, expectResult);
    expectResult = ToJsonString(jsonMap);
    EXPECT_EQ(expectResult, actualResult) << expectResult << '\n' << actualResult;
}

TEST_F(ConfigParserTest, testSquareInherit) {
    testParser(_configRoot, "square_inherit/expect_result.json", "square_inherit/derived.json");
}

TEST_F(ConfigParserTest, testWithDeepInherit) {
    testParser(_configRoot, "deep_inherit/expect_result.json", "deep_inherit/derived.json");
}

TEST_F(ConfigParserTest, testWithInherit) {
    testParser(_configRoot, "clusters/expect_result.json", "clusters/mainse_searcher_cluster.json");
}

TEST_F(ConfigParserTest, testWithSectionInherit) {
    testParser(_configRoot, "build/expect_result.json", "build/mainse_searcher_full_build.json");
}

TEST_F(ConfigParserTest, testClusterInheritAndSectionInherit) {
    testParser(_configRoot, "clusters/bad_expect_result.json",
               "clusters/mainse_searcher_bad_cluster.json");
}

TEST_F(ConfigParserTest, testWithExternalInherit) {
    string srcDir = _configRoot + "external_inherit";
    string fileDir = GET_TEST_DATA_PATH() + "external_inherit";
    FileUtil::removeIfExist(fileDir);
    FileUtil::atomicCopy(srcDir, fileDir);

    vector<string> fileList;
    FileUtil::listDir(fileDir, fileList);

    for (auto& file : fileList) {
        string filePath = FileUtil::joinFilePath(fileDir, file);
        replaceFile(filePath, "$ROOT_PATH", fileDir);
    }
    testParser(fileDir, "/expect_result.json", "/derived.json");
}
       
TEST_F(ConfigParserTest, testWithInheritLoop) {
    JsonMap jsonMap;
    ConfigParser parser(_configRoot);
    EXPECT_FALSE(parser.parse("", jsonMap));
    EXPECT_FALSE(parser.parse("invalid_configs/loop_cluster.json", jsonMap));
    EXPECT_FALSE(parser.parse("invalid_configs/invalid_json.json", jsonMap));
}

void ConfigParserTest::replaceFile(
    const string& filePath, const string& strToReplace,
    const string& replaceStr) {
    string content;
    FileUtil::readFile(filePath, content);
    auto pos = content.find(strToReplace);
    while (pos != string::npos) {
        content = content.replace(pos, strToReplace.size(), replaceStr);
        pos = content.find(strToReplace);
    }
    FileUtil::remove(filePath);
    FileUtil::writeFile(filePath, content);
}

}
}
