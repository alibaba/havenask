#include "build_service/reader/GZipFileReader.h"
#include "build_service/test/unittest.h"
#include "build_service/reader/GZipFileReader.h"
#include "build_service/util/FileUtil.h"
#include <zlib.h>

namespace build_service {
namespace reader {

class GZipFileReaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void aTest(const std::string & fileName, uint32_t fileSize,
               uint32_t blkSize, uint32_t checkSize);
};
using namespace build_service::util;
using namespace std;

void GZipFileReaderTest::setUp() {
}

void GZipFileReaderTest::tearDown() {
}

void GZipFileReaderTest::aTest(const string & fileName,
                               uint32_t fileSize,
                               uint32_t blkSize,
                               uint32_t checkSize)
{
    gzFile f = gzopen(fileName.c_str(), "wb");
    ASSERT_TRUE(f);

    for(uint32_t fi = 0 ; fi != fileSize; ++fi) {
        ASSERT_TRUE(-1 != gzputc(f, (unsigned char)(fi % (unsigned char)-1)));
    }
    ASSERT_EQ(Z_OK, gzclose(f));

    GZipFileReader gzip(blkSize);
    ASSERT_TRUE(gzip.init(fileName.c_str(),0));

    std::vector<char> output;
    output.resize(checkSize);
    uint32_t used = 0;
    uint32_t checked = 0;
    uint32_t cnt = 0;
    while(gzip.get(&*output.begin(), output.size(),used)) {
        ++cnt;
        ASSERT_TRUE(used);
        for(uint32_t j = 0; j < used; ++j) {
            unsigned char value = (unsigned char)(checked++ % (unsigned char)-1);
            ASSERT_EQ(value, (unsigned char)output[j]);
        }
    }
    ASSERT_TRUE(cnt > 0);
    ASSERT_EQ(checked, fileSize);
}

TEST_F(GZipFileReaderTest, testRead) {
    BS_LOG(DEBUG, "Begin Test!");

    string fileName = string(TEST_DATA_PATH) + "/tmpGZipFileReaderTest";
    aTest(fileName, 1024, 128, 64);
    aTest(fileName, 1024, 128, 2048);
    aTest(fileName, 1024, 53, 17);
    aTest(fileName, 1024, 53, 99);
    aTest(fileName, 1024, 2001, 55);
    aTest(fileName, 12*1024*1024, 10000, 1004);

    FileUtil::remove(fileName);
}

TEST_F(GZipFileReaderTest, testIsEof) {
    BS_LOG(DEBUG, "Begin Test!");
    string gzipFileName = string(TEST_DATA_PATH)
                          + "/raw_doc_files/standard_sample.data.gz";
    string normalFileName = string(TEST_DATA_PATH)
                            + "/raw_doc_files/standard_sample.data";
    string fileContent;
    ASSERT_TRUE(FileUtil::readFile(normalFileName, fileContent));
    int64_t totalSize = fileContent.length();

    GZipFileReader reader(100);
    bool ret = reader.init(gzipFileName, 0);
    ASSERT_TRUE(ret);

    int64_t sizeReadPerTime = 50;
    char* tmpBuffer = new char[sizeReadPerTime];

    int64_t totalReadSize = 0;
    uint32_t curReadSize = 0;
    bool curRet = false;
    string actualContent;
    while(true) {
        ASSERT_TRUE(!reader.isEof());
        curRet = reader.get(tmpBuffer, sizeReadPerTime, curReadSize);
        ret = ret && curRet;
        totalReadSize += curReadSize;
        actualContent += string(tmpBuffer, curReadSize);
        if (totalReadSize >= totalSize) {
            break;
        }
    }
    ASSERT_EQ(totalSize, totalReadSize);
    ASSERT_TRUE(reader.isEof());
    ASSERT_EQ(fileContent, actualContent);

    delete []tmpBuffer;
}

TEST_F(GZipFileReaderTest, testInitWithOffset) {
    BS_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH)
                      + "/raw_doc_files/standard_sample.data.gz";
    GZipFileReader reader(100);
    int64_t offset = 9;
    bool ret = reader.init(fileName, offset);
    ASSERT_TRUE(ret);

    string expectedContent = "id=1\n";
    uint32_t outputSize = expectedContent.length();
    char *tmpBuffer = new char[outputSize];

    uint32_t actualSize = 0;
    ret = reader.get(tmpBuffer, outputSize, actualSize);
    ASSERT_TRUE(ret);
    ASSERT_EQ(outputSize, actualSize);
    ASSERT_EQ(expectedContent, string(tmpBuffer, outputSize));
    ASSERT_FALSE(reader.isEof());

    delete []tmpBuffer;
}

TEST_F(GZipFileReaderTest, testInitWithOffsetLargerThanFileLength) {
    BS_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH)
                      + "/raw_doc_files/standard_sample.data.gz";
    GZipFileReader reader(100);
    int64_t offset = 369;
    bool ret = reader.init(fileName, offset);
    ASSERT_TRUE(ret);

    char tmpBuffer[10];

    uint32_t outputSize = 0;
    uint32_t actualSize = sizeof(tmpBuffer);
    ret = reader.get(tmpBuffer, outputSize, actualSize);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(reader.isEof());
}


TEST_F(GZipFileReaderTest, testRegular) {
    BS_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/testGzip.gz";
    GZipFileReader gzip(256);
    ASSERT_TRUE(gzip.init(fileName, 0));

    string content = "gzip is ok";
    std::vector<char> output;
    output.resize(1024);
    uint32_t used = 0;
    ASSERT_TRUE(gzip.get(&*output.begin(), output.size(),used));
    ASSERT_EQ(content.size(), size_t(used));
    ASSERT_TRUE(content == string(output.begin(), output.begin() + used));
}

TEST_F(GZipFileReaderTest, testInitWithEmptyGzipFile) {
    BS_LOG(DEBUG, "Begin Test!");

    string fileName = string(TEST_DATA_PATH) + "/emptyGzipFile.gz";
    GZipFileReader gzip(256);
    ASSERT_TRUE(gzip.init(fileName.c_str(), 0));

    std::vector<char> output;
    output.resize(128);
    uint32_t used = 0;
    ASSERT_FALSE(gzip.get(&*output.begin(), output.size(), used));
    ASSERT_EQ(size_t(0), size_t(used));
}

TEST_F(GZipFileReaderTest, testInitWithGzipFromEmptyFile) {
    BS_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/empty.data.gz";
    GZipFileReader gzip(256);
    ASSERT_TRUE(gzip.init(fileName.c_str(), 0));

    std::vector<char> output;
    output.resize(128);
    uint32_t used = 0;
    ASSERT_FALSE(gzip.get(&*output.begin(), output.size(), used));
    ASSERT_EQ(size_t(0), size_t(used));
}


}
}
