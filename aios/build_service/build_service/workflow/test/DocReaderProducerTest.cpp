#include "build_service/test/unittest.h"
#include "build_service/workflow/DocReaderProducer.h"
#include "build_service/reader/test/FakeRawDocumentReader.h"
#include "build_service/reader/test/FakeSwiftRawDocumentReader.h"
#include <autil/StringUtil.h>
#include <indexlib/util/counter/counter_map.h>

using namespace std;
using namespace autil;
using namespace build_service::reader;
using namespace build_service::common;
using namespace build_service::document;
namespace build_service {
namespace workflow {

class DocReaderProducerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void prepareDocument(reader::FakeRawDocumentReader *fakeReader, size_t docCount);
    void checkProduce(DocReaderProducer *producer, size_t startDoc, size_t docCount, bool endWithException = false);
protected:    
    ReaderInitParam _params;
};

void DocReaderProducerTest::setUp() {
    _params.counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
}

void DocReaderProducerTest::tearDown() {
}

TEST_F(DocReaderProducerTest, testProduce) {
    FakeRawDocumentReader fakeReader;
    ASSERT_TRUE(fakeReader.initialize(_params));
    size_t docCount = 10;
    uint64_t srcSignature = 0;
    prepareDocument(&fakeReader, docCount);
    DocReaderProducer producer(&fakeReader, srcSignature);
    checkProduce(&producer, 0, docCount);
}

TEST_F(DocReaderProducerTest, testSeek) {
    {
        FakeRawDocumentReader fakeReader;
        ASSERT_TRUE(fakeReader.initialize(_params));
        size_t docCount = 10;
        uint64_t srcSignature = 0;
        prepareDocument(&fakeReader, docCount);
        DocReaderProducer producer(&fakeReader, srcSignature);
        common::Locator locator(0, 3);
        producer.seek(locator);
        checkProduce(&producer, 4, docCount);
    }
    {
        FakeRawDocumentReader fakeReader;
        ASSERT_TRUE(fakeReader.initialize(_params));
        size_t docCount = 10;
        uint64_t srcSignature = 0;
        prepareDocument(&fakeReader, docCount);
        DocReaderProducer producer(&fakeReader, srcSignature);
        common::Locator locator(1, 3);
        producer.seek(locator);
        checkProduce(&producer, 0, docCount);
    }
}

TEST_F(DocReaderProducerTest, testGetMaxTimestamp) {
    {
        uint64_t srcSignature = 0;
        DocReaderProducer producer(NULL, srcSignature);
        int64_t timestamp;
        ASSERT_TRUE(!producer.getMaxTimestamp(timestamp));
    }
    {
        uint64_t srcSignature = 0;
        reader::FakeSwiftRawDocumentReader fakeSwiftReader;
        DocReaderProducer producer(&fakeSwiftReader, srcSignature);
        int64_t timestamp;
        ASSERT_TRUE(producer.getMaxTimestamp(timestamp));
        ASSERT_EQ(12138, timestamp);
    }
}

TEST_F(DocReaderProducerTest, testReadException) {
    FakeRawDocumentReader fakeReader;
    ASSERT_TRUE(fakeReader.initialize(_params));
    size_t docCount = 10;
    uint64_t srcSignature = 0;
    // total doc count 21, the 11th document read error;
    prepareDocument(&fakeReader, docCount);
    fakeReader.addRawDocument("", 0, true);
    prepareDocument(&fakeReader, docCount);
    DocReaderProducer producer(&fakeReader, srcSignature);
    checkProduce(&producer, 0, docCount, true);
}

void DocReaderProducerTest::prepareDocument(
        FakeRawDocumentReader *fakeReader, size_t docCount)
{
    for (size_t i = 0; i < docCount; ++i) {
        string identity = autil::StringUtil::toString(i);
        string docStr = "rawDocString=document_" + identity + "\n";
        fakeReader->addRawDocument(docStr, i);
    }
}

void DocReaderProducerTest::checkProduce(
        DocReaderProducer *producer, size_t startDoc, size_t docCount,
        bool endWithException)
{
    for (size_t i = startDoc; i < docCount; ++i) {
        string identity = autil::StringUtil::toString(i);
        RawDocumentPtr rawDoc;
        ASSERT_EQ(FE_OK, producer->produce(rawDoc));
        ASSERT_TRUE(rawDoc);
        EXPECT_EQ(Locator(i), rawDoc->getLocator());
        const string &rawDocStr = rawDoc->getField("rawDocString");
        EXPECT_EQ("document_"+identity, rawDocStr);
    }
    // double check

    FlowError ec = endWithException ? FE_FATAL : FE_EOF;
    RawDocumentPtr rawDoc;
    ASSERT_EQ(ec, producer->produce(rawDoc));
    ASSERT_TRUE(rawDoc);
    ASSERT_EQ(ec, producer->produce(rawDoc));
    ASSERT_TRUE(rawDoc);
}


}
}
