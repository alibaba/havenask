#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/BufferedPanguFileReader.h>
#include <suez/turing/common/FileUtil.h>

USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(build_system);

class BufferedPanguFileReaderTest : public TESTBASE {
public:
    BufferedPanguFileReaderTest();
    ~BufferedPanguFileReaderTest();
public:
    void setUp();
    void tearDown();
 
protected:
    char *_buffer;
    static const size_t BUFFER_LENGTH = 64;
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(build_system, BufferedPanguFileReaderTest);


BufferedPanguFileReaderTest::BufferedPanguFileReaderTest() { 
}

BufferedPanguFileReaderTest::~BufferedPanguFileReaderTest() { 
}

void BufferedPanguFileReaderTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _buffer = new char[BUFFER_LENGTH];
}

void BufferedPanguFileReaderTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete []_buffer;
}

TEST_F(BufferedPanguFileReaderTest, testReadLineEOF) {
    HA3_LOG(DEBUG, "Begin Test!");
    string file = "./tmp_for_testReadLineEOF";
    string content = "aaa=bbbbb\n";
    FileUtil::writeLocalFile(file, content);
    BufferedPanguFileReader pgFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)3, readCount);

    ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)6, readCount);

    ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_EOF, ret);
    FileUtil::removeLocalFile(file);
}

TEST_F(BufferedPanguFileReaderTest, testReadLineBeyondMaxLength) {
    HA3_LOG(DEBUG, "Begin Test!");
    string file = "testdata/tmp_for_testReadLineEOF";
    string content = "aaa=bbbbb\n";
    FileUtil::writeLocalFile(file, content);
    BufferedPanguFileReader pgFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)3, readCount);

    ret = pgFileReader.readLine(_buffer, 6, '\n', readCount);
    ASSERT_EQ(RE_BEYOND_MAXLENTH, ret);
    ASSERT_EQ((size_t)5, readCount);
    ASSERT_TRUE(strcmp(_buffer, "bbbbb") == 0);

    ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)2, readCount);

    ret = pgFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_EOF, ret);

    FileUtil::removeLocalFile(file);
}

END_HA3_NAMESPACE(build_system);

