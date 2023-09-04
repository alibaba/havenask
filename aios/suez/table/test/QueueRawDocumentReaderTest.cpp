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
        KeyValueMap kvMap;
        QueueRawDocumentReader reader;
        EXPECT_TRUE(!reader.initQueue(kvMap));
        EXPECT_TRUE(reader._docQueuePtr == nullptr);
    }
    {
        KeyValueMap kvMap;
        kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test";
        QueueRawDocumentReader reader;
        EXPECT_TRUE(reader.initQueue(kvMap));
        EXPECT_TRUE(reader._docQueuePtr != nullptr);
        EXPECT_EQ(string("test"), reader._queueName);
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
        KeyValueMap kvMap;
        kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test";
        QueueRawDocumentReader reader;
        EXPECT_TRUE(reader.initQueue(kvMap));
        string docStr;
        DocInfo docInfo;
        auto ec = reader.readDocStr(docStr, nullptr, docInfo);
        EXPECT_EQ(RawDocumentReader::ERROR_WAIT, ec);
    }
}

TEST_F(QueueRawDocumentReaderTest, testReadDocStrSuccess) {
    KeyValueMap kvMap;
    kvMap[QueueRawDocumentReader::QUEUE_NAME] = "test1";
    QueueRawDocumentReader reader;
    EXPECT_TRUE(reader.initQueue(kvMap));
    reader._docQueuePtr->Push("abc");
    string docStr;
    DocInfo docInfo;
    auto ec = reader.readDocStr(docStr, nullptr, docInfo);
    EXPECT_EQ(RawDocumentReader::ERROR_NONE, ec);
    EXPECT_EQ(string("abc"), docStr);
}

} // namespace suez
