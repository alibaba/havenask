#include "build_service/test/unittest.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"
#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace fslib;
using namespace autil;

using namespace build_service::util;
namespace build_service {
namespace reader {

class MockMultiFileDocumentReader : public MultiFileDocumentReader {
public:
    MockMultiFileDocumentReader(const CollectResults &fileList)
        : MultiFileDocumentReader(fileList)
    {}
protected:
    MOCK_METHOD0(calculateTotalFileSize, bool());
};

class MultiFileDocumentReaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();


protected:
    MultiFileDocumentReaderPtr createReader(const string &fileDocCountsStr);
    MultiFileDocumentReaderPtr createReaderForMulti(const int &fileNumber, const int &fileSize);
protected:
    std::string _rootPath;
};

class MockFileDocumentReader : public FileDocumentReader
{
public:
    MockFileDocumentReader(const map<string, int32_t> &fileDocs)
        : FileDocumentReader(1024 * 1024)
        , _curDocCount(0)
        , _curDocReadCount(0)
        , _fileDocs(fileDocs)
    {
        ON_CALL(*this, init(_, _)).WillByDefault(Invoke(std::bind(&MockFileDocumentReader::initForTest, this, std::placeholders::_1, std::placeholders::_2)));
        ON_CALL(*this, read(_)).WillByDefault(Invoke(std::bind(&MockFileDocumentReader::readForTest, this, std::placeholders::_1)));
        ON_CALL(*this, isEof()).WillByDefault(Invoke(std::bind(&MockFileDocumentReader::isEofForTest, this)));
        ON_CALL(*this, getFileOffset()).WillByDefault(Invoke(std::bind(&MockFileDocumentReader::getFileOffsetForTest, this)));
    }
    ~MockFileDocumentReader() {};
private:
    MOCK_METHOD2(init, bool(const string&, int64_t));
    MOCK_CONST_METHOD0(isEof, bool());
    MOCK_METHOD1(read, bool(string &));
    MOCK_CONST_METHOD0(getFileOffset, int64_t());
private:
    bool doRead(std::string& docStr) override { assert(false); return false; }

    bool readForTest(string &docStr) {
        if (_curDocReadCount < _curDocCount) {
            docStr = _curFile + "_doc_" + StringUtil::toString(_curDocReadCount);
            _curDocReadCount++;
            return true;
        }
        return false;
    }
    bool isEofForTest() const {
        return _curDocCount == _curDocReadCount;
    }
    bool initForTest(const string &fileName, int64_t offset) {
        map<string, int32_t>::const_iterator it = _fileDocs.find(fileName);
        if (it != _fileDocs.end()) {
            _curFile = fileName;
            _curDocCount = it->second;
            _curDocReadCount = 0;
            if (offset > _curDocCount) {
                return false;
            }
            _curDocReadCount = offset;
            return true;
        }
        return false;
    }
    int64_t getFileOffsetForTest() {
        return _curDocReadCount;
    }
private:
    string _curFile;
    int32_t _curDocCount;
    int32_t _curDocReadCount;
    map<string, int32_t> _fileDocs;
};

typedef ::testing::NiceMock<MockFileDocumentReader> NiceMockFileDocumentReader;

void MultiFileDocumentReaderTest::setUp() {
    _rootPath =GET_TEST_DATA_PATH();
}

void MultiFileDocumentReaderTest::tearDown() {
}

MultiFileDocumentReaderPtr MultiFileDocumentReaderTest::createReader(
        const string &fileDocCountsStr)
{
    vector<string> fileDocCounts = StringTokenizer::tokenize(ConstString(fileDocCountsStr), ",");
    CollectResults collectResults;
    map<string, int32_t> fileDocs;
    for (size_t i = 0; i < fileDocCounts.size(); ++i) {
        string fileName = "file_" + StringUtil::toString(i);
        collectResults.push_back(CollectResult((i+1)*(i+1), fileName));
        if (fileDocCounts[i] != "not_exist") {
            fileDocs[fileName] = StringUtil::fromString<int32_t>(fileDocCounts[i]);
        }
    }
    NiceMockFileDocumentReader *mockReader = new NiceMockFileDocumentReader(fileDocs);
    MultiFileDocumentReaderPtr reader(new MockMultiFileDocumentReader(collectResults));
    reader->_fileDocumentReaderPtr.reset(mockReader);
    return reader;
}
MultiFileDocumentReaderPtr MultiFileDocumentReaderTest::createReaderForMulti(
        const int &fileNumber, const int &fileSize)
{
    string fileDocCountsStr = "1";
    for (int i = 1; i < fileNumber; i++){
        fileDocCountsStr += ("," + StringUtil::toString(i));
    }
    vector<string> fileDocCounts = StringTokenizer::tokenize(ConstString(fileDocCountsStr), ",");
    CollectResults collectResults;
    map<string, int32_t> fileDocs;
    for (size_t i = 0; i < fileDocCounts.size(); ++i) {
        string fileName = "file_" + StringUtil::toString(i);
        collectResults.push_back(CollectResult((i+1)*(i+1), fileName));
        if (fileDocCounts[i] != "not_exist") {
            fileDocs[fileName] = StringUtil::fromString<int32_t>(fileDocCounts[i]);
        }
    }
    NiceMockFileDocumentReader *mockReader = new NiceMockFileDocumentReader(fileDocs);
    MultiFileDocumentReaderPtr reader(new MockMultiFileDocumentReader(collectResults));
    reader->_fileDocumentReaderPtr.reset(mockReader);
    return reader;
    
}

TEST_F(MultiFileDocumentReaderTest, testRead) {
    {
        MultiFileDocumentReaderPtr reader = createReader("1,2");
        string docStr;
        int64_t locator = 0;

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_0_doc_0"), docStr);
        int64_t fileId, fileOffset;
        reader->transLocatorToFileOffset(locator, fileId, fileOffset);
        EXPECT_EQ(int64_t(1), fileId);
        EXPECT_EQ(int64_t(1), fileOffset);

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_1_doc_0"), docStr);
        reader->transLocatorToFileOffset(locator, fileId, fileOffset);
        EXPECT_EQ(int64_t(4), fileId);
        EXPECT_EQ(int64_t(1), fileOffset);

        ASSERT_TRUE(reader->read(docStr, locator));
        reader->transLocatorToFileOffset(locator, fileId, fileOffset);
        EXPECT_EQ(int64_t(4), fileId);
        EXPECT_EQ(int64_t(2), fileOffset);
        ASSERT_TRUE(reader->isEof());
        EXPECT_EQ(string("file_1_doc_1"), docStr);
    }

    {
        MultiFileDocumentReaderPtr reader = createReader("0,2");
        string docStr;
        int64_t locator = 0;

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_1_doc_0"), docStr);

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_TRUE(reader->isEof());
        EXPECT_EQ(string("file_1_doc_1"), docStr);

    }

    {
        MultiFileDocumentReaderPtr reader = createReader("2");
        string docStr;
        int64_t locator = 0;

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_0_doc_0"), docStr);

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_TRUE(reader->isEof());
        EXPECT_EQ(string("file_0_doc_1"), docStr);
    }

    {
        MultiFileDocumentReaderPtr reader = createReader("2,-1");
        string docStr;
        int64_t locator = 0;

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_0_doc_0"), docStr);

        ASSERT_TRUE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
        EXPECT_EQ(string("file_0_doc_1"), docStr);

        ASSERT_FALSE(reader->read(docStr, locator));
        ASSERT_FALSE(reader->isEof());
    }
}

TEST_F(MultiFileDocumentReaderTest, testEmptyFileList) {
    MultiFileDocumentReaderPtr reader = createReader("");
    string docStr;
    int64_t locator = 0;

    ASSERT_FALSE(reader->read(docStr, locator));
    ASSERT_TRUE(reader->isEof());
}

TEST_F(MultiFileDocumentReaderTest, testSeek) {
    MultiFileDocumentReaderPtr reader = createReader("1,2,3,4");

    string docStr;

    int64_t locator1(0);
    ASSERT_TRUE(reader->read(docStr, locator1));
    EXPECT_EQ(string("file_0_doc_0"), docStr);

    int64_t locator2(0);
    ASSERT_TRUE(reader->read(docStr, locator2));
    EXPECT_EQ(string("file_1_doc_0"), docStr);

    int64_t locator3(0);
    ASSERT_TRUE(reader->read(docStr, locator3));
    EXPECT_EQ(string("file_1_doc_1"), docStr);

    {
        MultiFileDocumentReaderPtr newReader = createReader("1,2,3,4");
        ASSERT_TRUE(newReader->seek(locator1));
        int64_t locator = 0;
        ASSERT_TRUE(newReader->read(docStr, locator));
        EXPECT_EQ(string("file_1_doc_0"), docStr);
    }
    {
        MultiFileDocumentReaderPtr newReader = createReader("1,2,3,4");
        ASSERT_TRUE(newReader->seek(locator2));

        int64_t locator = 0;
        ASSERT_TRUE(newReader->read(docStr, locator));
        EXPECT_EQ(string("file_1_doc_1"), docStr);
    }
    {
        MultiFileDocumentReaderPtr newReader = createReader("1,2,3,4");
        ASSERT_TRUE(newReader->seek(locator3));

        int64_t locator = 0;
        ASSERT_TRUE(newReader->read(docStr, locator));
        EXPECT_EQ(string("file_2_doc_0"), docStr);
    }
}

TEST_F(MultiFileDocumentReaderTest, testInvalidSeek) {
    {
        MultiFileDocumentReaderPtr reader = createReader("1,2");
        int64_t locator = 0;
        reader->transFileOffsetToLocator(2, 0, locator);
        ASSERT_FALSE(reader->seek(locator));
    }

    {
        MultiFileDocumentReaderPtr reader = createReader("1,2");
        int64_t locator = 0;
        reader->transFileOffsetToLocator(0, 5, locator);
        ASSERT_FALSE(reader->seek(locator));
    }
}

TEST_F(MultiFileDocumentReaderTest, testSkipToNextFile) {
    {
        MultiFileDocumentReaderPtr reader = createReader("1,2");
        ASSERT_TRUE(reader->skipToNextFile());
        ASSERT_TRUE(reader->skipToNextFile());
        ASSERT_FALSE(reader->skipToNextFile());
    }

    {
        MultiFileDocumentReaderPtr reader = createReader("1,not_exist,2");
        ASSERT_TRUE(reader->skipToNextFile());
        ASSERT_FALSE(reader->skipToNextFile());
    }
}

TEST_F(MultiFileDocumentReaderTest, testTransLocatorToFileOffset) {
    MultiFileDocumentReaderPtr reader = createReader("1,2");
    string docStr;
    int64_t locator = 0;
    int64_t fileIndex;
    int64_t offset;

    ASSERT_TRUE(reader->read(docStr, locator));
    reader->transLocatorToFileOffset(locator, fileIndex, offset);
    EXPECT_EQ(1, fileIndex);
    EXPECT_EQ(1, offset);

    ASSERT_TRUE(reader->read(docStr, locator));
    reader->transLocatorToFileOffset(locator, fileIndex, offset);
    EXPECT_EQ(4, fileIndex);
    EXPECT_EQ(1, offset);

    ASSERT_TRUE(reader->read(docStr, locator));
    reader->transLocatorToFileOffset(locator, fileIndex, offset);
    ASSERT_EQ(4, fileIndex);
    ASSERT_EQ(2, offset);
}

TEST_F(MultiFileDocumentReaderTest, testTransformLocator) {
    int64_t fileIndex = 10;
    int64_t offset = 100;
    int64_t locator = 0;

    int64_t newFileIndex;
    int64_t newOffset;
    MultiFileDocumentReader::transFileOffsetToLocator(fileIndex, offset, locator);
    MultiFileDocumentReader::transLocatorToFileOffset(locator, newFileIndex, newOffset);
    EXPECT_EQ(fileIndex, newFileIndex);
    EXPECT_EQ(offset, newOffset);
}

TEST_F(MultiFileDocumentReaderTest, testMaxFileIndexLocator) {
    int64_t fileIndex = -1LL;
    int64_t offset = 100;
    int64_t locator = 0;

    int64_t newFileIndex;
    int64_t newOffset;
    MultiFileDocumentReader::transFileOffsetToLocator(fileIndex, offset, locator);
    MultiFileDocumentReader::transLocatorToFileOffset(locator, newFileIndex, newOffset);
    EXPECT_EQ(MultiFileDocumentReader::MAX_LOCATOR_FILE_INDEX, (uint64_t)newFileIndex);
    EXPECT_EQ(0, newOffset);
}

TEST_F(MultiFileDocumentReaderTest, testMaxOffsetLocator) {
    int64_t fileIndex = 100;
    int64_t offset = -1LL;
    int64_t locator = 0;

    int64_t newFileIndex;
    int64_t newOffset;
    MultiFileDocumentReader::transFileOffsetToLocator(fileIndex, offset, locator);
    MultiFileDocumentReader::transLocatorToFileOffset(locator, newFileIndex, newOffset);
    EXPECT_EQ(fileIndex, newFileIndex);
    EXPECT_EQ(MultiFileDocumentReader::MAX_LOCATOR_OFFSET, (uint64_t)newOffset);
}

TEST_F(MultiFileDocumentReaderTest, testMaxFileIdLocator) {
    int64_t fileId = MultiFileDocumentReader::MAX_LOCATOR_FILE_INDEX;
    int64_t offset = 1000;

    int64_t locator;
    MultiFileDocumentReader::transFileOffsetToLocator(fileId, offset, locator);

    int64_t newFileId = -1;
    int64_t newOffset = -1;
    MultiFileDocumentReader::transLocatorToFileOffset(locator, newFileId, newOffset);
    EXPECT_EQ(fileId, newFileId);
    EXPECT_EQ(offset, newOffset);

    fileId += 100;
    MultiFileDocumentReader::transFileOffsetToLocator(fileId, offset, locator);
    MultiFileDocumentReader::transLocatorToFileOffset(locator, newFileId, newOffset);
    EXPECT_EQ((int64_t)MultiFileDocumentReader::MAX_LOCATOR_FILE_INDEX, newFileId);
    EXPECT_EQ(int64_t(0), newOffset);
}

TEST_F(MultiFileDocumentReaderTest, testSkipToFileId) {
    {
        CollectResults results;
        MultiFileDocumentReader reader(results);
        reader.initForFormatDocument("<doc>", "</doc>");
        EXPECT_FALSE(reader.skipToFileId(0));
        EXPECT_FALSE(reader.skipToFileId(1));
        EXPECT_EQ(-1, reader._fileCursor);
    }
    {
        CollectResults results;
        results.push_back(CollectResult(2, "a"));
        results.push_back(CollectResult(4, "b"));
        results.push_back(CollectResult(7, "c"));
        MultiFileDocumentReader reader(results);
        reader.initForFormatDocument("<doc>", "</doc>");

        EXPECT_TRUE(reader.skipToFileId(2));
        EXPECT_EQ(0, reader._fileCursor);
        
        EXPECT_FALSE(reader.skipToFileId(3));
        EXPECT_EQ(-1, reader._fileCursor);

        EXPECT_TRUE(reader.skipToFileId(7));
        EXPECT_EQ(2, reader._fileCursor);

        EXPECT_FALSE(reader.skipToFileId(9));
        EXPECT_EQ(-1, reader._fileCursor);
    }
}

TEST_F(MultiFileDocumentReaderTest, testEstimateLeftTime) {
    MultiFileDocumentReaderPtr reader = createReader("2,3");
    MockMultiFileDocumentReader *mockReader = dynamic_cast<MockMultiFileDocumentReader*>(reader.get());
    ASSERT_TRUE(mockReader != NULL);

    // normal
    mockReader->_processedFileSize = 1000 * 10; // 10k
    mockReader->_totalFileSize = 1000 * 100; // 100k
    mockReader->_startTime = TimeUtility::currentTime() - 1000 * 1000;

    EXPECT_CALL(*mockReader, calculateTotalFileSize()).Times(0);
    int64_t freshness = mockReader->estimateLeftTime();
    EXPECT_NEAR(9000 * 1000, freshness, 1000 * 10);

    // leftSize = 0
    mockReader->_processedFileSize = 1000 * 10; // 10k
    mockReader->_totalFileSize = 1000 * 10; // 10k
    mockReader->_startTime = TimeUtility::currentTime() - 1000 * 1000;
    EXPECT_CALL(*mockReader, calculateTotalFileSize()).Times(0);
    freshness = mockReader->estimateLeftTime();
    EXPECT_EQ(0L, freshness);

    // fail
    mockReader->_totalFileSize = -1;
    mockReader->_startTime = TimeUtility::currentTime() - 1000 * 1000;
    EXPECT_CALL(*mockReader, calculateTotalFileSize()).WillOnce(Return(false));
    freshness = mockReader->estimateLeftTime();
    EXPECT_EQ(numeric_limits<int64_t>::max(), freshness);
}

TEST_F(MultiFileDocumentReaderTest, testEstimateLeftTimeOutOfint64) {
    MultiFileDocumentReaderPtr reader = createReader("2,3");
    MockMultiFileDocumentReader *mockReader = dynamic_cast<MockMultiFileDocumentReader*>(reader.get());
    ASSERT_TRUE(mockReader != NULL);


    // usedTime * leftSize (10Byte) > INT64_MAX 
    mockReader->_processedFileSize = INT64_MAX - 10; 
    mockReader->_totalFileSize = INT64_MAX;
    mockReader->_startTime = TimeUtility::currentTime() - (INT64_MAX - 1000);

    EXPECT_CALL(*mockReader, calculateTotalFileSize()).Times(0);

    int64_t freshness = mockReader->estimateLeftTime();
    EXPECT_NEAR(10, freshness, 1);
}

TEST_F(MultiFileDocumentReaderTest, testInit) {
    CollectResults collectResults;
    MultiFileDocumentReader reader(collectResults);
    ASSERT_TRUE(reader.initForFormatDocument("<doc>", "<doc/>"));

    ASSERT_TRUE(DYNAMIC_POINTER_CAST(FormatFileDocumentReader, reader._fileDocumentReaderPtr));

    ASSERT_FALSE(reader.initForFixLenBinaryDocument(0));
    ASSERT_TRUE(reader.initForFixLenBinaryDocument(5));
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(FixLenBinaryFileDocumentReader, reader._fileDocumentReaderPtr));
    
    ASSERT_TRUE(reader.initForVarLenBinaryDocument());
    ASSERT_TRUE(DYNAMIC_POINTER_CAST(VarLenBinaryFileDocumentReader, reader._fileDocumentReaderPtr));
}

}
}
