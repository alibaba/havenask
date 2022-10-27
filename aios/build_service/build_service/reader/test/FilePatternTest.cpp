#include "build_service/test/unittest.h"
#include "build_service/reader/FilePattern.h"

using namespace std;

namespace build_service {
namespace reader {

class FilePatternTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void FilePatternTest::setUp() {
}

void FilePatternTest::tearDown() {
}

TEST_F(FilePatternTest, testExtractFieldId) {
    {
        FilePattern pattern("", "common-", ".gz", 0);
        uint32_t fileId;
        EXPECT_TRUE(pattern.extractFileId("common-0.gz", fileId));
        EXPECT_EQ(uint32_t(0), fileId);
        EXPECT_FALSE(pattern.extractFileId("not_match-0.gz", fileId));
        EXPECT_FALSE(pattern.extractFileId("common-0not_match", fileId));
        EXPECT_FALSE(pattern.extractFileId("-0not_", fileId));
        EXPECT_FALSE(pattern.extractFileId("common-abc.gz", fileId));
        EXPECT_FALSE(pattern.extractFileId("common-99999999999.gz", fileId));
        EXPECT_FALSE(pattern.extractFileId("common--1.gz", fileId));
    }
    {
        FilePattern pattern("", "", "", 0);
        uint32_t fileId;
        EXPECT_TRUE(pattern.extractFileId("0", fileId));
        EXPECT_EQ(uint32_t(0), fileId);
        EXPECT_FALSE(pattern.extractFileId("not_match0", fileId));
        EXPECT_FALSE(pattern.extractFileId("0not_match", fileId));
    }
    {
        FilePattern pattern("", "common", "", 0);
        uint32_t fileId;
        EXPECT_TRUE(pattern.extractFileId("common0", fileId));
        EXPECT_EQ(uint32_t(0), fileId);
        EXPECT_FALSE(pattern.extractFileId("not_match0", fileId));
        EXPECT_FALSE(pattern.extractFileId("0not_match", fileId));
    }
    {
        FilePattern pattern("", "", "common", 0);
        uint32_t fileId;
        EXPECT_TRUE(pattern.extractFileId("0common", fileId));
        EXPECT_EQ(uint32_t(0), fileId);
        EXPECT_FALSE(pattern.extractFileId("not_match0", fileId));
        EXPECT_FALSE(pattern.extractFileId("0not_match", fileId));
    }
}

TEST_F(FilePatternTest, testCalculateHashIds) {
    {
        // pattern match
        FilePattern pattern("", "common-", ".gz", 8);
        CollectResults results;

        results.push_back(CollectResult("common-5.gz"));
        results.push_back(CollectResult("/////common-5.gz"));
        results.push_back(CollectResult("dir/////common-5.gz"));
        results.push_back(CollectResult("not_match-0.gz"));
        results = pattern.calculateHashIds(results);
        EXPECT_EQ(size_t(2), results.size());
        EXPECT_EQ("common-5.gz", results[0]._fileName);
        EXPECT_EQ("/////common-5.gz", results[1]._fileName);
    }
    {
        // dir not match
        FilePattern pattern("tmall", "common-", ".gz", 8);
        CollectResults results;
        pattern.setRootPath("/common");
        results.push_back(CollectResult("/common/tmall/common-5.gz"));
        results.push_back(CollectResult("/common////tmall////common-5.gz"));
        results.push_back(CollectResult("/taobao/tmall/common-5.gz"));
        results.push_back(CollectResult("unregular_file_name"));
        results = pattern.calculateHashIds(results);
        EXPECT_EQ(size_t(2), results.size());
        EXPECT_EQ("/common/tmall/common-5.gz", results[0]._fileName);
        EXPECT_EQ("/common////tmall////common-5.gz", results[1]._fileName);
    }
    {
        // range match
        FilePattern pattern("", "common-", ".gz", 8);
        CollectResults results;
        results.push_back(CollectResult("common-7.gz"));
        results.push_back(CollectResult("common-100.gz"));
        results = pattern.calculateHashIds(results);
        EXPECT_EQ(size_t(1), results.size());
        EXPECT_EQ("common-7.gz", results[0]._fileName);
    }
}


}
}
