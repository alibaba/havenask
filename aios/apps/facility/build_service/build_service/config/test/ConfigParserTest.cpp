#include "build_service/config/ConfigParser.h"

#include "autil/legacy/jsonizable.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::util;

namespace build_service { namespace config {

class ConfigParserTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    void testParser(const string& configRoot, const string& exepctResultFile, const string& testFile) const;
    void replaceFile(const string& filePath, const string& strToReplace, const string& replaceStr);

private:
    string _configRoot;
};

void ConfigParserTest::setUp() { _configRoot = GET_TEST_DATA_PATH() + "config_with_imports/"; }

void ConfigParserTest::tearDown() {}

void ConfigParserTest::testParser(const string& configRoot, const string& expectResultFile,
                                  const string& testFile) const
{
    JsonMap jsonMap;
    ConfigParser parser(configRoot);
    ASSERT_TRUE(parser.parse(testFile, jsonMap));
    string actualResult = ToJsonString(jsonMap);
    string expectResult;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(configRoot + expectResultFile, expectResult));
    FromJsonString(jsonMap, expectResult);
    expectResult = ToJsonString(jsonMap);
    EXPECT_EQ(expectResult, actualResult) << expectResult << '\n' << actualResult;
}

TEST_F(ConfigParserTest, testSquareInherit)
{
    testParser(_configRoot, "square_inherit/expect_result.json", "square_inherit/derived.json");
}

TEST_F(ConfigParserTest, testWithDeepInherit)
{
    testParser(_configRoot, "deep_inherit/expect_result.json", "deep_inherit/derived.json");
}

TEST_F(ConfigParserTest, testWithInherit)
{
    testParser(_configRoot, "clusters/expect_result.json", "clusters/mainse_searcher_cluster.json");
}

TEST_F(ConfigParserTest, testWithSectionInherit)
{
    testParser(_configRoot, "section_inherit/expect_result.json", "section_inherit/mainse_searcher_full_build.json");
}

TEST_F(ConfigParserTest, testClusterInheritAndSectionInherit)
{
    testParser(_configRoot, "clusters/bad_expect_result.json", "clusters/mainse_searcher_bad_cluster.json");
}

TEST_F(ConfigParserTest, testWithExternalInherit)
{
    string srcDir = _configRoot + "external_inherit";
    string fileDir = GET_TEMP_DATA_PATH() + "external_inherit";
    fslib::util::FileUtil::removeIfExist(fileDir);
    fslib::util::FileUtil::atomicCopy(srcDir, fileDir);

    vector<string> fileList;
    fslib::util::FileUtil::listDir(fileDir, fileList);

    for (auto& file : fileList) {
        string filePath = fslib::util::FileUtil::joinFilePath(fileDir, file);
        replaceFile(filePath, "$ROOT_PATH", fileDir);
    }
    testParser(fileDir, "/expect_result.json", "/derived.json");
}

TEST_F(ConfigParserTest, testWithInheritLoop)
{
    JsonMap jsonMap;
    ConfigParser parser(_configRoot);
    EXPECT_FALSE(parser.parse("", jsonMap));
    EXPECT_FALSE(parser.parse("invalid_configs/loop_cluster.json", jsonMap));
    EXPECT_FALSE(parser.parse("invalid_configs/invalid_json.json", jsonMap));
}

void ConfigParserTest::replaceFile(const string& filePath, const string& strToReplace, const string& replaceStr)
{
    string content;
    fslib::util::FileUtil::readFile(filePath, content);
    auto pos = content.find(strToReplace);
    while (pos != string::npos) {
        content = content.replace(pos, strToReplace.size(), replaceStr);
        pos = content.find(strToReplace);
    }
    fslib::util::FileUtil::remove(filePath);
    fslib::util::FileUtil::writeFile(filePath, content);
}

}} // namespace build_service::config
