#include "build_service/util/ZipFileUtil.h"

#include <iosfwd>
#include <string>
#include <unordered_map>

#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class ZipFileUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ZipFileUtilTest::setUp() {}

void ZipFileUtilTest::tearDown() {}

TEST_F(ZipFileUtilTest, testReadZipFile)
{
    string zipFile = GET_TEST_DATA_PATH() + "/resource_reader_test/zip_config/schemas.zip";
    unordered_map<string, string> fileMap;
    ASSERT_TRUE(ZipFileUtil::readZipFile(zipFile, fileMap));
    ASSERT_EQ(5, fileMap.size());
    ASSERT_EQ(25174, fileMap["schemas/cluster1_schema.json"].size());
    ASSERT_EQ(25174, fileMap["schemas/cluster2_schema.json"].size());
    ASSERT_EQ(25174, fileMap["schemas/cluster3_schema.json"].size());
    ASSERT_EQ(25174, fileMap["schemas/cluster4_schema.json"].size());
    ASSERT_EQ(25186, fileMap["schemas/cluster5_schema.json"].size());
}

TEST_F(ZipFileUtilTest, testReadException)
{
    unordered_map<string, string> fileMap;
    // non exist file
    ASSERT_FALSE(ZipFileUtil::readZipFile("non-exist-file.zip", fileMap));
    // error format
    ASSERT_FALSE(ZipFileUtil::readZipFile(GET_TEST_DATA_PATH() + "/util_test/clusters.tar", fileMap));
    // broken zip file
    ASSERT_FALSE(ZipFileUtil::readZipFile(GET_TEST_DATA_PATH() + "/util_test/broken-zip-file.tar", fileMap));
}

}} // namespace build_service::util
