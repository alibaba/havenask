#include "build_service/test/unittest.h"
#include "build_service/reader/FileRawDocumentReader.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/Separator.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/util/FileUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/util/counter/state_counter.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(document);
using namespace build_service::document;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service {
namespace reader {

class FileRawDocumentReaderTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    void checkSimpleData(const string &fileName);
    void checkSimpleData(const string &fileName,
                         size_t startId, int64_t locator,
                         size_t saveLocatorId, int64_t &saveLocator);
    void checkSimpleDoc(size_t docId, const RawDocument &rawDoc);
private:
    void innerTestRead(const std::string &prefix, const std::string &suffix,
                       const std::string &fieldSep, const std::string &keyValueSep);
    void innerTestForMultiFiles(
            const std::string &prefix, const std::string &suffix,
            const std::string &fieldSep, const std::string &keyValueSep);
    std::string createFile(const std::string &fileName, const std::string &content);
    std::string generateDocString(
            const std::string &prefix, const std::string &suffix,
            const std::string &fieldSep, const std::string &keyValueSep,
            const std::string &fieldValue, const std::string &fieldName="fieldName");
private:
    std::string _rootPath;
    std::string _casePath;
};

void FileRawDocumentReaderTest::setUp() {
    _rootPath = TEST_DATA_PATH"/raw_doc_files/";
    _casePath = GET_TEST_DATA_PATH();
}

void FileRawDocumentReaderTest::tearDown() {
}

TEST_F(FileRawDocumentReaderTest, testInit) {
    {
        FileRawDocumentReader reader;
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "=";
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "\n";
        params.kvMap[DATA_PATH] = _rootPath;
        params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = ";";
        params.kvMap[SRC_SIGNATURE] = "1";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
        params.counterMap = counterMap;
        EXPECT_TRUE(reader.initialize(params));
    }
    {
        // no document key value seperator
        FileRawDocumentReader reader;
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "";
        params.kvMap[DATA_PATH] = _rootPath;
        params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = ";";
        params.kvMap[SRC_SIGNATURE] = "1";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
        params.counterMap = counterMap;
        EXPECT_FALSE(reader.initialize(params));
    }
    {
        // no data path
        FileRawDocumentReader reader;
        ReaderInitParam params;
        params.kvMap[RAW_DOCUMENT_KV_SEP] = "=";
        params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "\n";
        params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = ";";
        params.kvMap[SRC_SIGNATURE] = "1";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
        params.counterMap = counterMap;
        EXPECT_FALSE(reader.initialize(params));
    }
    {
        // no document suffix
        FileRawDocumentReader reader;
        ReaderInitParam params;
        params.kvMap[DATA_PATH] = _rootPath;
        params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "";
        params.kvMap[SRC_SIGNATURE] = "1";
        EXPECT_FALSE(reader.initialize(params));
    }
    {
        // src signature invalid
        FileRawDocumentReader reader;
        ReaderInitParam params;
        params.kvMap[DATA_PATH] = _rootPath;
        params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "";
        params.kvMap[SRC_SIGNATURE] = "abv";
        EXPECT_FALSE(reader.initialize(params));
    }
}

TEST_F(FileRawDocumentReaderTest, testCreateMultiFileDocumentReader) {
    CollectResults collectResults;
    {
        FileRawDocumentReader reader;
        KeyValueMap kvMap;
        kvMap[RAW_DOCUMENT_FORMAT] = "";
        kvMap[RAW_DOCUMENT_SEP_PREFIX] = "a";
        kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "b";
        kvMap[SRC_SIGNATURE] = "0";
        ReaderInitParam params;
        params.kvMap = kvMap;
        EXPECT_FALSE(reader.initialize(params));
        MultiFileDocumentReader *fileReader = reader.createMultiFileDocumentReader(collectResults, kvMap);
        EXPECT_TRUE(fileReader);

        FormatFileDocumentReaderPtr docReader =
            DYNAMIC_POINTER_CAST(FormatFileDocumentReader, fileReader->_fileDocumentReaderPtr);
        ASSERT_TRUE(docReader.get());
        EXPECT_EQ("a", docReader->_docPrefix._sep);
        EXPECT_EQ("b", docReader->_docSuffix._sep);
        delete fileReader;
    }
    {
        FileRawDocumentReader reader;
        KeyValueMap kvMap;
        kvMap[RAW_DOCUMENT_FORMAT] = "isearch";
        kvMap[RAW_DOCUMENT_SEP_PREFIX] = "a";
        kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "b";
        kvMap[SRC_SIGNATURE] = "0";
        ReaderInitParam params;
        params.kvMap = kvMap;
        EXPECT_FALSE(reader.initialize(params));
        MultiFileDocumentReader *fileReader = reader.createMultiFileDocumentReader(collectResults, kvMap);
        EXPECT_TRUE(fileReader);

        FormatFileDocumentReaderPtr docReader =
            DYNAMIC_POINTER_CAST(FormatFileDocumentReader, fileReader->_fileDocumentReaderPtr);
        ASSERT_TRUE(docReader.get());
        EXPECT_EQ(RAW_DOCUMENT_ISEARCH_SEP_PREFIX, docReader->_docPrefix._sep);
        EXPECT_EQ(RAW_DOCUMENT_ISEARCH_SEP_SUFFIX, docReader->_docSuffix._sep);
        delete fileReader;
    }
    {
        FileRawDocumentReader reader;
        KeyValueMap kvMap;
        kvMap[RAW_DOCUMENT_FORMAT] = "ha3";
        kvMap[RAW_DOCUMENT_SEP_PREFIX] = "a";
        kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "b";
        kvMap[SRC_SIGNATURE] = "0";
        ReaderInitParam params;
        params.kvMap = kvMap;
        EXPECT_FALSE(reader.initialize(params));
        MultiFileDocumentReader *fileReader = reader.createMultiFileDocumentReader(collectResults, kvMap);
        EXPECT_TRUE(fileReader);

        FormatFileDocumentReaderPtr docReader =
            DYNAMIC_POINTER_CAST(FormatFileDocumentReader, fileReader->_fileDocumentReaderPtr);
        ASSERT_TRUE(docReader.get());

        EXPECT_EQ(RAW_DOCUMENT_HA3_SEP_PREFIX, docReader->_docPrefix._sep);
        EXPECT_EQ(RAW_DOCUMENT_HA3_SEP_SUFFIX, docReader->_docSuffix._sep);
        delete fileReader;
    }
    {
        FileRawDocumentReader reader;
        KeyValueMap kvMap;
        kvMap[RAW_DOCUMENT_FORMAT] = "unknown";
        kvMap[RAW_DOCUMENT_SEP_PREFIX] = "a";
        kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "b";
        kvMap[SRC_SIGNATURE] = "0";
        ReaderInitParam params;
        params.kvMap = kvMap;
        EXPECT_FALSE(reader.initialize(params));
        MultiFileDocumentReader *fileReader = reader.createMultiFileDocumentReader(collectResults, kvMap);
        EXPECT_TRUE(fileReader);

        FormatFileDocumentReaderPtr docReader =
            DYNAMIC_POINTER_CAST(FormatFileDocumentReader, fileReader->_fileDocumentReaderPtr);
        ASSERT_TRUE(docReader.get());

        EXPECT_EQ("a", docReader->_docPrefix._sep);
        EXPECT_EQ("b", docReader->_docSuffix._sep);
        delete fileReader;
    }
}

TEST_F(FileRawDocumentReaderTest, testReadHa3Doc) {
    checkSimpleData("standard_sample.data");
    checkSimpleData("standard_sample.data.gz");
}

TEST_F(FileRawDocumentReaderTest, testReadException) {
    FileRawDocumentReader reader;
    ReaderInitParam params;
    params.kvMap[DATA_PATH] = _rootPath + "standard_sample.data";
    params.kvMap[SRC_SIGNATURE] = "1";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    params.counterMap = counterMap;
    ASSERT_TRUE(reader.initialize(params));

    reader._documentReader->_fileList.push_back(CollectResult(0, _rootPath + "not_exist_file"));
    int64_t offset;
    for (size_t i = 0; i < 4; ++i) {
        DefaultRawDocument rawDoc(reader._hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
    }
    DefaultRawDocument rawDoc(reader._hashMapManager);
    ASSERT_EQ(RawDocumentReader::ERROR_EXCEPTION, reader.read(rawDoc, offset));
}

TEST_F(FileRawDocumentReaderTest, testCounters) {
    FileRawDocumentReader reader;
    ReaderInitParam params;
    params.kvMap[DATA_PATH] = _rootPath + "standard_sample.data";
    params.kvMap[SRC_SIGNATURE] = "1";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    params.counterMap = counterMap;
    ASSERT_TRUE(reader.initialize(params));

    reader._documentReader->_fileList.push_back(CollectResult(0, _rootPath + "not_exist_file"));
    int64_t offset;
    for (size_t i = 0; i < 4; ++i) {
        DefaultRawDocument rawDoc(reader._hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
    }

    int64_t estimateLeftTime = reader._documentReader->estimateLeftTime() / (1000 * 1000);
    ASSERT_EQ(estimateLeftTime, GET_STATE_COUNTER(counterMap, processor.estimateLeftTime)->Get());
}


TEST_F(FileRawDocumentReaderTest, testReadMultiIsearch5File) {
    FileRawDocumentReader reader;
    ReaderInitParam params;
    params.kvMap[RAW_DOCUMENT_SEP_PREFIX] = "<doc>\x01\n";
    params.kvMap[RAW_DOCUMENT_SEP_SUFFIX] = "</doc>\x01\n";
    params.kvMap[RAW_DOCUMENT_FIELD_SEP] = "\x01\n";
    params.kvMap[RAW_DOCUMENT_KV_SEP] = "=";
    params.kvMap[DATA_PATH] = _rootPath + "isearch_doc.data" + ","
                               + _rootPath + "isearch_doc.data.gz";
    params.kvMap[SRC_SIGNATURE] = "1";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    params.counterMap = counterMap;
    ASSERT_TRUE(reader.initialize(params));

    int64_t offset;
    for (size_t i = 0; i < 8; ++i) {
        DefaultRawDocument rawDoc(reader._hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset)) << i;
        checkSimpleDoc(i % 4, rawDoc);
    }
    DefaultRawDocument rawDoc(reader._hashMapManager);
    ASSERT_EQ(RawDocumentReader::ERROR_EOF, reader.read(rawDoc, offset));
}

void FileRawDocumentReaderTest::checkSimpleData(const string &fileName) {
    int64_t useless(0);
    int64_t doc2Locator(0);
    checkSimpleData(fileName, 0, useless, 2, doc2Locator);

    checkSimpleData(fileName, 3, doc2Locator, -1, useless);
}

void FileRawDocumentReaderTest::checkSimpleData(const string &fileName,
        size_t startId, int64_t locator, size_t saveLocatorId, 
        int64_t &saveLocator)
{
    FileRawDocumentReader reader;
    ReaderInitParam params;
    params.kvMap[DATA_PATH] = _rootPath + fileName;
    params.kvMap[SRC_SIGNATURE] = "1";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    params.counterMap = counterMap;
    ASSERT_TRUE(reader.initialize(params));

    ASSERT_TRUE(reader.seek(locator));

    int64_t offset;
    for (size_t i = startId; i < 4; ++i) {
        DefaultRawDocument rawDoc(reader._hashMapManager);
        ASSERT_EQ(RawDocumentReader::ERROR_NONE, reader.read(rawDoc, offset));
        checkSimpleDoc(i, rawDoc);
        if (i == saveLocatorId) {
            saveLocator = offset;
        }
    }
    DefaultRawDocument rawDoc(reader._hashMapManager);
    ASSERT_EQ(RawDocumentReader::ERROR_EOF, reader.read(rawDoc, offset));
}

void FileRawDocumentReaderTest::checkSimpleDoc(size_t docId, const RawDocument &rawDoc) {
    ASSERT_EQ(uint32_t(6), rawDoc.getFieldCount());
    string id = StringUtil::toString(docId+1);
    string multiValue;
    for (size_t i = 0; i < 3; ++i) {
        multiValue += StringUtil::toString(docId+i+1);
        if (i != 2) {
            multiValue += '';
        }
    }
    EXPECT_EQ(id, rawDoc.getField("id"));
    EXPECT_EQ(multiValue, rawDoc.getField("cat_id"));
}

}
}
