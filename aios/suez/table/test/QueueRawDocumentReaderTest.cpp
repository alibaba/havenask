#include "suez/table/QueueRawDocumentReader.h"

#include <unittest/unittest.h>

using namespace std;
using namespace testing;
using namespace build_service;
using namespace build_service::reader;

namespace suez {

class QueueRawDocumentReaderTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void QueueRawDocumentReaderTest::setUp() {}

void QueueRawDocumentReaderTest::tearDown() {}

TEST_F(QueueRawDocumentReaderTest, testInitQueue) {
    {
        ReaderInitParam params;
        QueueRawDocumentReader reader;
        EXPECT_TRUE(!reader.initQueue(params));
        EXPECT_TRUE(reader._docQueuePtr == nullptr);
    }
    {
        ReaderInitParam params;
        params.range.set_from(0);
        params.range.set_to(32767);
        auto& kvMap = params.kvMap;
        kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test";
        QueueRawDocumentReader reader;
        EXPECT_TRUE(reader.initQueue(params));
        EXPECT_TRUE(reader._docQueuePtr != nullptr);
        EXPECT_EQ(string("test_0_32767"), reader._queueName);
    }
}

TEST_F(QueueRawDocumentReaderTest, testReadDocStrFailed) {
    {
        QueueRawDocumentReader reader;
        string docStr;
        DocInfo docInfo;
        auto ec = reader.readDocStr(docStr, nullptr, docInfo);
        EXPECT_EQ(RawDocumentReader::ERROR_EXCEPTION, ec);
    }
    {
        ReaderInitParam params;
        params.range.set_from(0);
        params.range.set_to(32767);
        auto& kvMap = params.kvMap;
        kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test";
        QueueRawDocumentReader reader;
        EXPECT_TRUE(reader.initQueue(params));
        string docStr;
        DocInfo docInfo;
        auto ec = reader.readDocStr(docStr, nullptr, docInfo);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
    }
}

TEST_F(QueueRawDocumentReaderTest, testReadDocStrSuccess) {
    ReaderInitParam params;
    params.range.set_from(0);
    params.range.set_to(32767);
    auto& kvMap = params.kvMap;
    kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test1";
    QueueRawDocumentReader reader;
    EXPECT_TRUE(reader.initQueue(params));
    reader._docQueuePtr->Push({0, string("abc")});
    string docStr;
    DocInfo docInfo;
    Checkpoint checkpoint;
    auto ec = reader.readDocStr(docStr, &checkpoint, docInfo);
    EXPECT_EQ(RawDocumentReader::ERROR_NONE, ec);
    EXPECT_EQ(string("abc"), docStr);
}

} // namespace suez
