#include "build_service/reader/test/FileDocumentReaderTestBase.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/test/test.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service {
namespace reader {

void FileDocumentReaderTestBase::testReadEmptyFile() {
    ASSERT_TRUE(FileUtil::writeFile(_fileName, ""));
    FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
    ASSERT_TRUE(reader.init(_fileName, 0));
    string docString;
    ASSERT_FALSE(reader.read(docString));
    ASSERT_TRUE(reader.isEof());
}

void FileDocumentReaderTestBase::testReadEmptyDoc() {
    ASSERT_TRUE(FileUtil::writeFile(_fileName, _docPrefix + _docSuffix));
    FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
    ASSERT_TRUE(reader.init(_fileName, 0));
    string docString;
    ASSERT_TRUE(reader.read(docString));
    int64_t fileOffset = _docPrefix.length() + _docSuffix.length();
    ASSERT_EQ((int64_t)fileOffset, reader.getFileOffset());
    ASSERT_TRUE(!reader.isEof());

    ASSERT_TRUE(!reader.read(docString));
    ASSERT_EQ((int64_t)fileOffset, reader.getFileOffset());
    ASSERT_TRUE(reader.isEof());
}

void FileDocumentReaderTestBase::testSeek() {
    ASSERT_TRUE(FileUtil::writeFile(_fileName, _docPrefix + "abc" + _docSuffix));
    FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
    ASSERT_TRUE(reader.init(_fileName, 0));
    string doc1, doc2;
    ASSERT_TRUE(reader.read(doc1));
    int64_t fileOffset = _docPrefix.length() + _docSuffix.length() + 3;
    ASSERT_EQ((int64_t)fileOffset, reader.getFileOffset());
    ASSERT_TRUE(reader.seek(0));
    ASSERT_EQ(0, reader.getFileOffset());
    ASSERT_TRUE(reader.read(doc2));
    EXPECT_EQ(doc1, doc2);
    ASSERT_EQ((int64_t)fileOffset, reader.getFileOffset());
}

void FileDocumentReaderTestBase::checkReadDoc(uint32_t docCount, uint32_t docSize) {
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    bool endWithSep = true;
    bool gzip = true;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, 0, 0, docStringVect, offsetVect);

    endWithSep = false;
    gzip = true;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, 0, 0, docStringVect, offsetVect);

    endWithSep = false;
    gzip = false;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, 0, 0, docStringVect, offsetVect);

    endWithSep = true;
    gzip = false;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, 0, 0, docStringVect, offsetVect);
}

void FileDocumentReaderTestBase::testReadOneDoc() {
    BS_LOG(DEBUG, "Begin Test!");
    checkReadDoc(1, _bufferSize / 3);
    checkReadDoc(1, _bufferSize * 2 + 1);
    checkReadDoc(1, _bufferSize);
    checkReadDoc(1, _bufferSize + 1);
    checkReadDoc(1, _bufferSize + 2);
}

void FileDocumentReaderTestBase::testReadMultiDocs() {
    BS_LOG(DEBUG, "Begin Test!");

    checkReadDoc(100, _bufferSize / 3);
    checkReadDoc(100, _bufferSize);
    checkReadDoc(100, _bufferSize * 2 + 1);
}

void FileDocumentReaderTestBase::checkReadDocWithOffset(
        uint32_t docCount, uint32_t docSize)
{
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    bool endWithSep = true;
    bool gzip = false;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[docCount - 1], docCount,
                         docStringVect, offsetVect);

    endWithSep = true;
    gzip = true;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[docCount - 1], docCount,
                         docStringVect, offsetVect);

    endWithSep = false;
    gzip = true;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[docCount - 1], docCount,
                         docStringVect, offsetVect);

    endWithSep = false;
    gzip = false;
    makeDocumentFile(docCount, docSize, endWithSep, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(endWithSep, offsetVect[docCount - 1], docCount,
                         docStringVect, offsetVect);
}

void FileDocumentReaderTestBase::testReadMultiDocsWithOffset() {
    BS_LOG(DEBUG, "Begin Test!");

    uint32_t docCount = 100;
    uint32_t docSize = _bufferSize / 3;
    checkReadDocWithOffset(docCount, docSize);

    docSize = _bufferSize * 2 + 1;
    checkReadDocWithOffset(docCount, docSize);
}

void FileDocumentReaderTestBase::testReadOffsetPastEof() {
    BS_LOG(DEBUG, "Begin Test!");

    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize / 3;
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    {
        makeDocumentFile(docCount, docSize, false, false, docStringVect, offsetVect);
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(!reader.init(_fileName, docSize * 100));
    }
    {
        makeDocumentFile(docCount, docSize, false, true, docStringVect, offsetVect);
        FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
        ASSERT_TRUE(!reader.init(_fileName, docSize * 100));
    }
}

void FileDocumentReaderTestBase::internalCheckReadDoc(bool endWithSep, int64_t offset,
        int32_t startDocCount, vector<string>& docStringVect,
        vector<int64_t>& offsetVect)
{
    FormatFileDocumentReader reader(_docPrefix, _docSuffix, _bufferSize);
    ASSERT_TRUE(reader.init(_fileName, offset));
    string docString;
    string expectString;
    int32_t docCount = docStringVect.size();
    for (int32_t i = startDocCount; i < docCount; i++) {
        if (!endWithSep && i == docCount - 1) {
            ASSERT_TRUE(!reader.read(docString));
            ASSERT_EQ(offsetVect[i], reader.getFileOffset());
            ASSERT_TRUE(reader.isEof());
            break;
        }

        ASSERT_TRUE(reader.read(docString));
        ASSERT_EQ((uint32_t)(i - startDocCount + 1), reader.getReadDocCount());
        expectString = docStringVect[i];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[i], reader.getFileOffset());
        ASSERT_TRUE(!reader.isEof());
    }

    if (endWithSep) {
        ASSERT_TRUE(!reader.read(docString));
        ASSERT_EQ(offsetVect[docCount - 1], reader.getFileOffset());
        ASSERT_TRUE(reader.isEof());
    }
}

void FileDocumentReaderTestBase::makeDocumentFile(uint32_t docCount,
        uint32_t docContentPrefixLen, bool endWithSep, bool gzip,
        vector<string> &docStringVect, vector<int64_t>& offsetVect)
{
    docStringVect.clear();
    offsetVect.clear();
    _fileName = _baseDir + "/FileDocumentReaderTestBase";
    makeOneDocumentFile(_fileName, docCount, docContentPrefixLen,
                        endWithSep, docStringVect, offsetVect);
    if (gzip) {
        string cmd = string("gzip -f ") + _fileName;
        system(cmd.c_str());
        _fileName = _fileName + ".gz";
    }
}

void FileDocumentReaderTestBase::makeOneDocumentFile(
        const string& fileName, uint32_t docCount,
        uint32_t contentLen, bool endWithSep,
        vector<string>& docStringVect, vector<int64_t>& offsetVect)
{
    string docString;
    int32_t fullDocCount = docCount;
    if (!endWithSep) {
        fullDocCount--;
    }

    int64_t offset = 0;
    string content;
    uint32_t prefixLen = _docPrefix.length();
    uint32_t suffixLen = _docSuffix.length();
    for (int32_t i = 0; i < fullDocCount; i++) {
        docString = makeOneDocument(contentLen) + StringUtil::toString(i);
        content += _docPrefix + docString + _docSuffix;
        docStringVect.push_back(docString);
        offset += prefixLen + docString.length() + suffixLen;
        offsetVect.push_back(offset);
    }

    // no suffix
    if (!endWithSep && docCount > 0) {
        docString = _docPrefix + makeOneDocument(contentLen);
        content += docString;
        docStringVect.push_back(docString);
        offset += docString.length();
        offsetVect.push_back(offset);
    }

    ASSERT_TRUE(FileUtil::writeFile(fileName, content));
}

string FileDocumentReaderTestBase::makeOneDocument(uint32_t len) {
    string doc;
    doc.reserve(len);
    for (uint32_t i = 0; i < len; ++i) {
        doc.push_back(char(i % 26 + 'a'));
    }
    return doc;
}

}
}
