#include "build_service/reader/test/BinaryFileDocumentReaderTestBase.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/VarLenBinaryDocumentEncoder.h"
#include "build_service/test/test.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service {
namespace reader {

void BinaryFileDocumentReaderTestBase::testReadEmptyFile() {
    ASSERT_TRUE(FileUtil::writeFile(_fileName, ""));
    FileDocumentReaderPtr reader = createFileDocumentReader();
    ASSERT_TRUE(reader->init(_fileName, 0));
    string docString;
    ASSERT_FALSE(reader->read(docString));
    ASSERT_TRUE(reader->isEof());
}

void BinaryFileDocumentReaderTestBase::testReadEmptyDoc() {
    if (_isFixLen) {
        return;
    }
    
    string emptyDocStr = VarLenBinaryDocumentEncoder::encode(string(""));
    ASSERT_TRUE(FileUtil::writeFile(_fileName, emptyDocStr));
    FileDocumentReaderPtr reader = createFileDocumentReader();
    ASSERT_TRUE(reader->init(_fileName, 0));
    string docString;
    ASSERT_TRUE(reader->read(docString));
    int64_t fileOffset = emptyDocStr.size();
    ASSERT_EQ((int64_t)fileOffset, reader->getFileOffset());
    ASSERT_TRUE(!reader->isEof());

    ASSERT_TRUE(!reader->read(docString));
    ASSERT_EQ((int64_t)fileOffset, reader->getFileOffset());
    ASSERT_TRUE(reader->isEof());
}

void BinaryFileDocumentReaderTestBase::checkReadDoc(uint32_t docCount, uint32_t docSize) {
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    if (_isFixLen) {
        _fixLen = docSize;
    }
    bool gzip = true;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(0, 0, docStringVect, offsetVect);

    gzip = false;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(0, 0, docStringVect, offsetVect);
}

void BinaryFileDocumentReaderTestBase::testReadOneDoc() {
    BS_LOG(DEBUG, "Begin Test!");
    checkReadDoc(1, _bufferSize / 3);
    checkReadDoc(1, _bufferSize * 2 + 1);
    checkReadDoc(1, _bufferSize);
    checkReadDoc(1, _bufferSize + 1);
    checkReadDoc(1, _bufferSize + 2);
}

void BinaryFileDocumentReaderTestBase::testReadMultiDocs() {
    BS_LOG(DEBUG, "Begin Test!");

    checkReadDoc(100, _bufferSize / 3);
    checkReadDoc(100, _bufferSize);
    checkReadDoc(100, _bufferSize * 2 + 1);
}

void BinaryFileDocumentReaderTestBase::checkReadDocWithOffset(
        uint32_t docCount, uint32_t docSize)
{
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    if (_isFixLen) {
        _fixLen = docSize;
    }

    bool gzip = false;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(offsetVect[docCount - 1], docCount,
                         docStringVect, offsetVect);

    gzip = true;
    makeDocumentFile(docCount, docSize, gzip, docStringVect, offsetVect);
    internalCheckReadDoc(offsetVect[0], 1, docStringVect, offsetVect);
    internalCheckReadDoc(offsetVect[docCount - 1], docCount, docStringVect, offsetVect);
}

void BinaryFileDocumentReaderTestBase::testReadMultiDocsWithOffset() {
    BS_LOG(DEBUG, "Begin Test!");

    uint32_t docCount = 100;
    uint32_t docSize = _bufferSize / 3;
    checkReadDocWithOffset(docCount, docSize);

    docSize = _bufferSize * 2 + 1;
    checkReadDocWithOffset(docCount, docSize);
}

void BinaryFileDocumentReaderTestBase::testReadOffsetPastEof() {
    BS_LOG(DEBUG, "Begin Test!");

    uint32_t docCount = 10;
    uint32_t docSize = _bufferSize / 3;
    vector<string> docStringVect;
    vector<int64_t> offsetVect;

    {
        makeDocumentFile(docCount, docSize, false, docStringVect, offsetVect);
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(!reader->init(_fileName, docSize * 100));
    }
    {
        makeDocumentFile(docCount, docSize, true, docStringVect, offsetVect);
        FileDocumentReaderPtr reader = createFileDocumentReader();
        ASSERT_TRUE(!reader->init(_fileName, docSize * 100));
    }
}

void BinaryFileDocumentReaderTestBase::internalCheckReadDoc(
        int64_t offset, int32_t startDocCount,
        vector<string>& docStringVect, vector<int64_t>& offsetVect)
{
    FileDocumentReaderPtr reader = createFileDocumentReader();
    ASSERT_TRUE(reader->init(_fileName, offset));
    string docString;
    string expectString;
    int32_t docCount = docStringVect.size();
    
    for (int32_t i = startDocCount; i < docCount; i++) {
        ASSERT_TRUE(reader->read(docString)) << i << "," << docCount;
        ASSERT_EQ((uint32_t)(i - startDocCount + 1), reader->getReadDocCount());
        expectString = docStringVect[i];
        ASSERT_EQ(expectString, docString);
        ASSERT_EQ(offsetVect[i], reader->getFileOffset());
        ASSERT_TRUE(!reader->isEof());
    }
    ASSERT_TRUE(!reader->read(docString));
    ASSERT_EQ(offsetVect[docCount - 1], reader->getFileOffset());
    ASSERT_TRUE(reader->isEof());
}

void BinaryFileDocumentReaderTestBase::makeVarLenBinaryDocumentFile(
        uint32_t docCount, uint32_t contentLen, bool gzip,
        vector<string> &docStringVect, vector<int64_t>& offsetVect)
{
    docStringVect.clear();
    offsetVect.clear();
    _fileName = _baseDir + "/BinaryFileDocumentReaderTestBase_varlen";
    makeOneVarLenDocumentFile(_fileName, docCount, contentLen,
                              docStringVect, offsetVect);
    if (gzip) {
        string cmd = string("gzip -f ") + _fileName;
        system(cmd.c_str());
        _fileName = _fileName + ".gz";
    }
}

void BinaryFileDocumentReaderTestBase::makeFixLenBinaryDocumentFile(
        uint32_t docCount, uint32_t contentLen, bool gzip,
        vector<string> &docStringVect, vector<int64_t>& offsetVect)
{
    docStringVect.clear();
    offsetVect.clear();
    _fileName = _baseDir + "/BinaryFileDocumentReaderTestBase_fixlen";
    makeOneFixLenDocumentFile(_fileName, docCount, contentLen,
                              docStringVect, offsetVect);
    if (gzip) {
        string cmd = string("gzip -f ") + _fileName;
        system(cmd.c_str());
        _fileName = _fileName + ".gz";
    }
}

void BinaryFileDocumentReaderTestBase::makeOneFixLenDocumentFile(
        const string& fileName, uint32_t docCount, uint32_t contentLen, 
        vector<string>& docStringVect, vector<int64_t>& offsetVect)
{
    string docString;
    int64_t offset = 0;
    string content;
    for (int32_t i = 0; i < (int32_t)docCount; i++) {
        docString = makeOneDocument(contentLen, i);
        content += docString;
        docStringVect.push_back(docString);
        offset += docString.size();
        offsetVect.push_back(offset);
    }
    ASSERT_TRUE(FileUtil::writeFile(fileName, content));
}

void BinaryFileDocumentReaderTestBase::makeOneVarLenDocumentFile(
        const string& fileName, uint32_t docCount, uint32_t contentLen, 
        vector<string>& docStringVect, vector<int64_t>& offsetVect)
{
    string docString;
    int64_t offset = 0;
    string content;
    string encodeStr;
    for (int32_t i = 0; i < (int32_t)docCount; i++) {
        docString = makeOneDocument(contentLen, i);
        encodeStr = VarLenBinaryDocumentEncoder::encode(docString);
        content += encodeStr;
        docStringVect.push_back(docString);
        offset += encodeStr.size();
        offsetVect.push_back(offset);
    }
    ASSERT_TRUE(FileUtil::writeFile(fileName, content));
}

string BinaryFileDocumentReaderTestBase::makeOneDocument(uint32_t len, uint32_t idx)
{
    if (len == 0) {
        return string("");
    }
    if (len == 1) {
        return StringUtil::toString(idx).substr(0, 1);
    }
    return makeOneDocument(len - 1) + StringUtil::toString(idx).substr(0, 1);
}

string BinaryFileDocumentReaderTestBase::makeOneDocument(uint32_t len) {
    string doc;
    doc.reserve(len);
    for (uint32_t i = 0; i < len; ++i) {
        doc.push_back(char(i % 26 + 'a'));
    }
    return doc;
}

FileDocumentReaderPtr BinaryFileDocumentReaderTestBase::createFileDocumentReader() {
    FileDocumentReaderPtr reader;
    if (_isFixLen) {
        reader.reset(new FixLenBinaryFileDocumentReader(_fixLen, _bufferSize));
    } else {
        reader.reset(new VarLenBinaryFileDocumentReader(_bufferSize));
    }
    return reader;
}

void BinaryFileDocumentReaderTestBase::makeDocumentFile(
        uint32_t docCount, uint32_t contentLen, bool gzip,
        vector<string> &docStringVect, vector<int64_t>& offsetVect) {
    if (_isFixLen) {
        _fixLen = contentLen;
        makeFixLenBinaryDocumentFile(docCount, contentLen,
                gzip, docStringVect, offsetVect);
    } else {
        makeVarLenBinaryDocumentFile(docCount, contentLen,
                gzip, docStringVect, offsetVect);
    }
}

}
}
