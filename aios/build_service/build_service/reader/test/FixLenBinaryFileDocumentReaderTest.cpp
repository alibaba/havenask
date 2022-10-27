#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/reader/test/BinaryFileDocumentReaderTestBase.h"
#include "build_service/test/test.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::util;
namespace build_service {
namespace reader {

class FixLenBinaryFileDocumentReaderTest : public BinaryFileDocumentReaderTestBase
{
public:
    void setUp();
    void tearDown();
protected:
    void checkReadRandomOffset(bool gzip);
    void checkReadFail(uint32_t contentLen, bool gzip);
};

void FixLenBinaryFileDocumentReaderTest::setUp() {
    _bufferSize = 10;
    _baseDir = GET_TEST_DATA_PATH();
    _fileName = _baseDir + "/BinaryFileDocumentReaderTest";
    _isFixLen = true;
    _fixLen = 15;
}

void FixLenBinaryFileDocumentReaderTest::tearDown() {
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadException) {
    testReadEmptyFile();
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadRandomOffsetSmallBufferSize) {
    _bufferSize = 10;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadFail) {
    // fixLen = 15, doc length not match
    checkReadFail(10, true);
    checkReadFail(10, false);
    checkReadFail(20, true);
    checkReadFail(20, false);
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadRandomOffsetLargeBufferSize) {
    _bufferSize = 100;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadOneDoc) {
    _bufferSize = 60;
    testReadOneDoc();
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadMultiDocs) {
    _bufferSize = 75;
    testReadMultiDocs();
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadMultiDocsWithOffset) {
    _bufferSize = 90;
    testReadMultiDocsWithOffset();
}

TEST_F(FixLenBinaryFileDocumentReaderTest, testReadOffsetPastEof) {
    _bufferSize = 50;
    testReadOffsetPastEof();
}

void FixLenBinaryFileDocumentReaderTest::checkReadFail(uint32_t contentLen, bool gzip) {
    vector<string> docStringVec;
    vector<int64_t> offsetVec;
    makeFixLenBinaryDocumentFile(1, contentLen, gzip, docStringVec, offsetVec);
    FileDocumentReaderPtr reader = createFileDocumentReader();
    ASSERT_TRUE(reader->init(_fileName, 0));
    string docString;
    while (contentLen - (uint32_t)reader->getFileOffset() >= (uint32_t)_fixLen) {
        ASSERT_TRUE(reader->read(docString));
        ASSERT_FALSE(reader->isEof());
    }

    if (contentLen - reader->getFileOffset() > 0) {
        ASSERT_FALSE(reader->read(docString));
        ASSERT_TRUE(reader->isEof());
    }
}

void FixLenBinaryFileDocumentReaderTest::checkReadRandomOffset(bool gzip) {
    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize * 2 + 1;
    _fixLen = docSize;
    
    vector<string> docStringVect;
    vector<int64_t> offsetVect;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);

    {
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(reader->init(_fileName, offsetVect[docCount / 2]));
        string docString;
        ASSERT_TRUE(reader->read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader->getFileOffset());
        ASSERT_TRUE(!reader->isEof());
    }
    {
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(reader->init(_fileName, offsetVect[docCount / 2] - 2));
        string docString;
        ASSERT_TRUE(reader->read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader->getFileOffset());
        ASSERT_TRUE(!reader->isEof());
    }
    {
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(reader->init(_fileName, offsetVect[docCount / 2] - 1));
        string docString;
        ASSERT_TRUE(reader->read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader->getFileOffset());
        ASSERT_TRUE(!reader->isEof());
    }
}


}
}
