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

class VarLenBinaryFileDocumentReaderTest : public BinaryFileDocumentReaderTestBase
{
public:
    void setUp();
    void tearDown();
protected:
    void checkReadRandomOffset(bool gzip);
};

void VarLenBinaryFileDocumentReaderTest::setUp() {
    _bufferSize = 10;
    _baseDir = GET_TEST_DATA_PATH();
    _fileName = _baseDir + "/BinaryFileDocumentReaderTest";
    _isFixLen = false;
}

void VarLenBinaryFileDocumentReaderTest::tearDown() {
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadException) {
    testReadEmptyFile();
    testReadEmptyDoc();
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadRandomOffsetSmallBufferSize) {
    _bufferSize = 10;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadRandomOffsetLargeBufferSize) {
    _bufferSize = 100;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadOneDoc) {
    _bufferSize = 60;
    testReadOneDoc();
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadMultiDocs) {
    _bufferSize = 75;
    testReadMultiDocs();
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadMultiDocsWithOffset) {
    _bufferSize = 90;
    testReadMultiDocsWithOffset();
}

TEST_F(VarLenBinaryFileDocumentReaderTest, testReadOffsetPastEof) {
    _bufferSize = 50;
    testReadOffsetPastEof();
}

void VarLenBinaryFileDocumentReaderTest::checkReadRandomOffset(bool gzip) {
    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize * 2 + 1;
    
    vector<string> docStringVect;
    vector<int64_t> offsetVect;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);

    {
        // valid offset point to the header of one binary document
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
        // invalid offset, will read from beginner
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(reader->init(_fileName, offsetVect[docCount / 2] - 2));
        string docString;
        ASSERT_TRUE(reader->read(docString));
        
        string expectString = docStringVect[0];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[0], reader->getFileOffset());
        ASSERT_TRUE(!reader->isEof());
    }
}


}
}
