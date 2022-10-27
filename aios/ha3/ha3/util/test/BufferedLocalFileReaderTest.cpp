#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/BufferedLocalFileReader.h>
#include <suez/turing/common/FileUtil.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(build_system);

class BufferedLocalFileReaderTest : public TESTBASE {
public:
    BufferedLocalFileReaderTest();
    ~BufferedLocalFileReaderTest();
public:
    void setUp();
    void tearDown();
protected:
    char* _buffer;
    static const size_t BUFFER_LENGTH = 64;
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(build_system, BufferedLocalFileReaderTest);


BufferedLocalFileReaderTest::BufferedLocalFileReaderTest() { 
}

BufferedLocalFileReaderTest::~BufferedLocalFileReaderTest() { 
}

void BufferedLocalFileReaderTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _buffer = new char[BUFFER_LENGTH];
}

void BufferedLocalFileReaderTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete []_buffer;
}

TEST_F(BufferedLocalFileReaderTest, testReadLineEOF) {
    HA3_LOG(DEBUG, "Begin Test!");

    string file = "testdata/testReadLineEOF";
    string content = "CMD=delete\n";
    FileUtil::writeLocalFile(file, content);

    BufferedLocalFileReader localFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)3, readCount);

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)7, readCount);

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_EOF, ret);

    FileUtil::removeLocalFile(file);
}
TEST_F(BufferedLocalFileReaderTest, testReadLineWithoutLastDelim) {
    HA3_LOG(DEBUG, "Begin testReadLineWithoutLastDelim!");
    
    string file = "testdata/testReadLineWithoutLastDelim";
    FileUtil::removeLocalFile(file);
    string content("aa\nbb", 5);
    FileUtil::writeLocalFile(file, content);

    BufferedLocalFileReader localFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)2, readCount);
    ASSERT_EQ(string("aa"), string(_buffer, readCount));

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)2, readCount);
    ASSERT_EQ(string("bb"), string(_buffer, readCount));

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_EOF, ret);
    FileUtil::removeLocalFile(file);
}

TEST_F(BufferedLocalFileReaderTest, testReadLineWithNil) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    string file = "testdata/testReadLineWithNil";
    FileUtil::removeLocalFile(file);
    string content("aa\nbb\0\0bb\ncc\n", 13);
    FileUtil::writeLocalFile(file, content);

    BufferedLocalFileReader localFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ(string("aa"), string(_buffer, readCount));

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)6, readCount);
    ASSERT_EQ(string("bb\0\0bb", 6), string(_buffer, readCount));

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ(string("cc"), string(_buffer, readCount));

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '\n', readCount);
    ASSERT_EQ(RE_EOF, ret);

    FileUtil::removeLocalFile(file);
}


TEST_F(BufferedLocalFileReaderTest, testReadLineBeyondMaxLength) {
    HA3_LOG(DEBUG, "Begin Test!");

    string file = "testdata/testReadLineBeyondMaxLength";
    string content = "CMD=delete\n";
    FileUtil::writeLocalFile(file, content);

    BufferedLocalFileReader localFileReader(file);

    size_t readCount = 0;
    ReadErrorCode ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)3, readCount);

    ret = localFileReader.readLine(_buffer, 7, '\n', readCount);
    ASSERT_EQ(RE_BEYOND_MAXLENTH, ret);
    ASSERT_EQ((size_t)6, readCount);

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_NO_ERROR, ret);
    ASSERT_EQ((size_t)2, readCount);

    ret = localFileReader.readLine(_buffer, BUFFER_LENGTH, '=', readCount);
    ASSERT_EQ(RE_EOF, ret);

    FileUtil::removeLocalFile(file);
}

END_HA3_NAMESPACE(build_system);

