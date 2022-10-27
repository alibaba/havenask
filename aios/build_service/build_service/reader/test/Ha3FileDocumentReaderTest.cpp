#include "build_service/test/unittest.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/test/FileDocumentReaderTestBase.h"
#include "build_service/reader/test/FakeFileReader.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace reader {

class Ha3FileDocumentReaderTest : public FileDocumentReaderTestBase {
public:
    void setUp();
    void tearDown();
protected:
    void checkReadRandomOffset(bool gzip);
};

void Ha3FileDocumentReaderTest::setUp() {
    _bufferSize = 0;
    _docPrefix = "";
    _docSuffix = "\x1E\n";
    _baseDir = GET_TEST_DATA_PATH();
    _fileName = _baseDir + "/FileDocumentReaderTest";
}

void Ha3FileDocumentReaderTest::tearDown() {
}

TEST_F(Ha3FileDocumentReaderTest, testResumeReadAfterReadFail) {
    _bufferSize = 10;
    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize * 2 + 1;
    vector<string> docStringVect;
    vector<int64_t> offsetVect;
    makeDocumentFile(docCount, docSize, false, false, docStringVect, offsetVect);
    FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
    ASSERT_TRUE(reader.init(_fileName, 0));
    FakeFileReader *fakeReader = new FakeFileReader;
    reader._fReader.reset(fakeReader);
    ASSERT_TRUE(reader._fReader->init(_fileName, 0));
    ASSERT_TRUE(reader._fReader->good());

    string docString;
    ASSERT_TRUE(reader.read(docString));
    EXPECT_EQ(docStringVect[0], docString);
    EXPECT_TRUE(!reader.isEof());
    EXPECT_EQ(offsetVect[0], reader.getFileOffset());

    fakeReader->setFail(true);
    docString.clear();
    ASSERT_FALSE(reader.read(docString));
    EXPECT_TRUE(docString.empty());
    EXPECT_TRUE(!reader.isEof());
    EXPECT_EQ(offsetVect[0], reader.getFileOffset());

    fakeReader->setFail(false);
    docString.clear();
    ASSERT_TRUE(reader.read(docString));
    EXPECT_EQ(docStringVect[1], docString);
    EXPECT_TRUE(!reader.isEof());
    EXPECT_EQ(offsetVect[1], reader.getFileOffset());
}

TEST_F(Ha3FileDocumentReaderTest, testReadRandomOffsetSmallBufferSize) {
    _bufferSize = 10;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(Ha3FileDocumentReaderTest, testReadRandomOffsetLargeBufferSize) {
    _bufferSize = 100;
    checkReadRandomOffset(false);
    checkReadRandomOffset(true);
}

TEST_F(Ha3FileDocumentReaderTest, testReadException) {
    testReadEmptyFile();
    testReadEmptyDoc();
}

void Ha3FileDocumentReaderTest::checkReadRandomOffset(bool gzip) {
    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize * 2 + 1;
    vector<string> docStringVect;
    vector<int64_t> offsetVect;
    makeDocumentFile(docCount, docSize, false, gzip, docStringVect, offsetVect);

    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2] + 1));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        string expectString = docStringVect[docCount / 2 + 1].substr(1);
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }

    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2] - 2));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        ASSERT_EQ("", docString);
        ASSERT_EQ(offsetVect[docCount / 2], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }

    {
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(reader.init(_fileName, offsetVect[docCount / 2] - 1));
        string docString;
        ASSERT_TRUE(reader.read(docString));
        string expectString = _docSuffix.substr(1) +
                              docStringVect[docCount / 2 + 1];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[docCount / 2 + 1], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }
}

}
}
