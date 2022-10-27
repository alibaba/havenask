#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/test/FileDocumentReaderTestBase.h"
#include "build_service/test/test.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::util;
namespace build_service {
namespace reader {

class ISearchFileDocumentReaderTest : public FileDocumentReaderTestBase
{
public:
    void setUp();
    void tearDown();
protected:
    void checkReadRandomOffset(bool gzip);
};

void ISearchFileDocumentReaderTest::setUp() {
    _bufferSize =  0;
    _docPrefix = "<doc>\x01\n";
    _docSuffix = "</doc>\x01\n";
    _baseDir = GET_TEST_DATA_PATH();
    _fileName = _baseDir + "/FileDocumentReaderTest";
}

void ISearchFileDocumentReaderTest::tearDown() {
}

TEST_F(ISearchFileDocumentReaderTest, testReadException) {
    testReadEmptyFile();
    testReadEmptyDoc();
}

TEST_F(ISearchFileDocumentReaderTest, testReadRandomOffsetSmallBufferSize) {
    _bufferSize = 10;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(ISearchFileDocumentReaderTest, testReadRandomOffsetLargeBufferSize) {
    _bufferSize = 100;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

void ISearchFileDocumentReaderTest::checkReadRandomOffset(bool gzip) {
    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize * 2 + 1;
    vector<string> docStringVect;
    vector<int64_t> offsetVect;
    makeDocumentFile(docCount, docSize, false, gzip, docStringVect, offsetVect);

    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2]));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }
    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2] - 2));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }
    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2] - 1));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        string expectString = docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }
}


}
}
