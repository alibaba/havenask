#include "build_service/test/unittest.h"
#include "build_service/processor/BatchProcessor.h"
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/test/ReusedDocumentProcessor.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::common;

IE_NAMESPACE_USE(util);

namespace build_service {
namespace processor {

class BatchProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
    
protected:
    RawDocumentVecPtr createRawDocVecWithSubDocs(const string &rawDocString);
    void checkProcessedDocs(const ProcessedDocumentVecPtr& docs,
                            const string& pDocInfoStr);

protected:
    proto::PartitionId _partitionId;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};

void BatchProcessorTest::setUp() {
    _partitionId.Clear();
    _partitionId.mutable_buildid()->set_datatable("simple");
    *_partitionId.add_clusternames() = "simple_cluster";
    *_partitionId.add_clusternames() = "simple_no_summary_cluster";
    _counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
}

void BatchProcessorTest::tearDown() {
}

TEST_F(BatchProcessorTest, testStart) {
    // test invalid param
    {
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

        BatchProcessor processor1("batch_size=abc");
        ASSERT_FALSE(processor1.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
        
        BatchProcessor processor2("batch_size=20;max_enqueue_seconds=-10");
        ASSERT_FALSE(processor2.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        BatchProcessor processor3("batch_size=20;max_enqueue_seconds=10;invalid_param=xxx");
        ASSERT_FALSE(processor3.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
    }
    
    // test valid param
    {
        BatchProcessor processor("batch_size=20;max_enqueue_seconds=2");
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(2), processor._chains->size());

        ASSERT_TRUE(processor._loopThreadPtr);
        ASSERT_EQ(20, processor._batchSize);
        ASSERT_EQ(2000000, processor._maxEnQueueTime);
    }
}

TEST_F(BatchProcessorTest, testProcessDoc) {
    // test MultiThread process
    {
        BatchProcessor processor("batch_size=15;max_enqueue_seconds=2");
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        size_t docCount = 100;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        uint32_t batchCount = 0;
        for (size_t i = 0; i < docCount; ) {
            document::ProcessedDocumentVecPtr docs = processor.getProcessedDoc();
            ASSERT_TRUE(docs->size() % 2 == 0);
            for (size_t j = 0; j < docs->size() / 2; j++) {
                document::ProcessedDocumentVecPtr checkDocs(new ProcessedDocumentVec);
                checkDocs->push_back((*docs)[j * 2]);
                checkDocs->push_back((*docs)[j * 2 + 1]);
                DocumentTestHelper::checkDocument(i++, checkDocs);
            }
            batchCount++;
        }
        
        // (100 + 14) / 15 = 7
        ASSERT_EQ(7, batchCount);

        // test no memory leak after stop
        processor.stop();
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docCount);
        processor.processDoc(rawDoc);
    }
    // test SingleThread process
    {
        BatchProcessor processor("batch_size=15;max_enqueue_seconds=2");
        string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        size_t docCount = 100;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        uint32_t batchCount = 0;
        for (size_t i = 0; i < docCount; ) {
            document::ProcessedDocumentVecPtr docs = processor.getProcessedDoc();
            ASSERT_TRUE(docs->size() % 2 == 0);
            for (size_t j = 0; j < docs->size() / 2; j++) {
                document::ProcessedDocumentVecPtr checkDocs(new ProcessedDocumentVec);
                checkDocs->push_back((*docs)[j * 2]);
                checkDocs->push_back((*docs)[j * 2 + 1]);
                DocumentTestHelper::checkDocument(i++, checkDocs);
            }
            batchCount++;
        }
        // (100 + 14) / 15 = 7
        ASSERT_EQ(7, batchCount);

        processor.stop();
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docCount);
        processor.processDoc(rawDoc);        
    }    
}

TEST_F(BatchProcessorTest, testProcessorWithChainSelector) 
{
    BatchProcessor processor("batch_size=10");
    string configRoot = TEST_DATA_PATH"processor_test/config_with_chain_selector/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

    {
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(1);
        rawDoc->setField("field", "f1");
        rawDoc->setField("section", "s1");
        processor.processDoc(rawDoc);

        ProcessedDocumentVecPtr docVec = processor.getProcessedDoc();
        ASSERT_EQ(1, docVec->size());
        ASSERT_EQ(0, processor._processErrorCounter->Get());
        ASSERT_EQ(1, processor._processDocCountCounter->Get());
        DocumentTestHelper::checkDocument(
                FakeDocument(StringUtil::toString(1), ADD_DOC, true, true),
                (*docVec)[0]->getDocument());
        common::Locator locator(1);
        EXPECT_EQ(locator, (*docVec)[0]->getLocator());
    }
    {
        // test process doc without chains
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(2);
        rawDoc->setField("field", "not-exist");
        rawDoc->setField("section", "s1");
        processor.processDoc(rawDoc);

        RawDocumentPtr rawDoc1 = DocumentTestHelper::createRawDocStr(3);
        rawDoc1->setField("field", "f2");
        rawDoc1->setField("section", "s2");
        processor.processDoc(rawDoc1);

        ProcessedDocumentVecPtr docVec = processor.getProcessedDoc();
        ASSERT_EQ(1, docVec->size());
        ASSERT_EQ(1, processor._processErrorCounter->Get());
        ASSERT_EQ(3, processor._processDocCountCounter->Get());
        DocumentTestHelper::checkDocument(
                FakeDocument(StringUtil::toString(3), ADD_DOC, false, true),
                (*docVec)[0]->getDocument());
        common::Locator locator(3);
        EXPECT_EQ(locator, (*docVec)[0]->getLocator());
    }
    processor.stop();
}

TEST_F(BatchProcessorTest, testStop) {
    {
        BatchProcessor processor("batch_size=32;max_enqueue_seconds=4");
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        size_t docCount = 10;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        // stop will trigger FlushDocQueue
        processor.stop();
        ProcessedDocumentVecPtr docs = processor.getProcessedDoc();
        EXPECT_TRUE(docs);
        EXPECT_EQ(docCount * 2, docs->size());
        EXPECT_TRUE(!processor.getProcessedDoc());
    }
    {
        BatchProcessor processor("batch_size=32;max_enqueue_seconds=4");
        string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
        size_t docCount = 10;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        processor.stop();
        ProcessedDocumentVecPtr docs = processor.getProcessedDoc();
        EXPECT_TRUE(docs);
        EXPECT_EQ(docCount * 2, docs->size());
        EXPECT_TRUE(!processor.getProcessedDoc());
    }    
}

TEST_F(BatchProcessorTest, testBatchProcessMetricsAndCounter)
{
    BatchProcessor processor("batch_size=5;max_enqueue_seconds=10");
    string configRoot = TEST_DATA_PATH"processor_test/config_batch_process/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
    
    int64_t flushTime = processor._lastFlushTime;
    int64_t flushTime2 = 0;
    {
        for (size_t i = 0; i < 3; i++)
        {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(1);
            rawDoc->setField("field", "f1");
            rawDoc->setField("section", "s1");
            processor.processDoc(rawDoc);
        }
        ASSERT_EQ(processor._lastFlushTime, flushTime);
        for (size_t i = 0; i < 4; i++)
        {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(2);
            rawDoc->setField("field", "f2");
            rawDoc->setField("section", "s2");
            processor.processDoc(rawDoc);
        }
        // flush once
        ASSERT_TRUE(processor._lastFlushTime > 0);
        ASSERT_GT(processor._lastFlushTime, flushTime);
        flushTime = processor._lastFlushTime;
        ProcessedDocumentVecPtr docVec = processor.getProcessedDoc();
        ASSERT_EQ(5, docVec->size());
        ASSERT_EQ(0, processor._processErrorCounter->Get());
        ASSERT_EQ(5, processor._processDocCountCounter->Get());
        DocumentTestHelper::checkDocument(
                FakeDocument("1", ADD_DOC, true, true),
                (*docVec)[0]->getDocument());
        DocumentTestHelper::checkDocument(
                FakeDocument("2", ADD_DOC, false, true),
                (*docVec)[3]->getDocument());

        EXPECT_EQ(common::Locator(1), (*docVec)[0]->getLocator());
        EXPECT_EQ(common::Locator(2), (*docVec)[3]->getLocator());
    }
    {
        for (size_t i = 0; i < 6; i++)
        {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(3);
            rawDoc->setField("field", "f3");
            rawDoc->setField("section", "s3");
            processor.processDoc(rawDoc);
        }
        for (size_t i = 0; i < 1; i++)
        {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(4, "abc");
            rawDoc->setField("field", "f3");
            rawDoc->setField("section", "s3");
            processor.processDoc(rawDoc);
        }
        // flush twice
        ASSERT_TRUE(processor._lastFlushTime > flushTime);
        flushTime2 = processor._lastFlushTime;
        
        ProcessedDocumentVecPtr docVec = processor.getProcessedDoc();
        ASSERT_EQ(5, docVec->size());
        ASSERT_EQ(0, processor._processErrorCounter->Get());
        ASSERT_EQ(10, processor._processDocCountCounter->Get());
        DocumentTestHelper::checkDocument(
                FakeDocument("2", ADD_DOC, false, true),
                (*docVec)[0]->getDocument());
        DocumentTestHelper::checkDocument(
                FakeDocument("3", ADD_DOC, false, true),
                (*docVec)[3]->getDocument());
        EXPECT_EQ(common::Locator(2), (*docVec)[0]->getLocator());
        EXPECT_EQ(common::Locator(3), (*docVec)[3]->getLocator());
    }
    {   
        // wait 10s to flush queue
        sleep(2);
        ASSERT_EQ(flushTime2, processor._lastFlushTime);
        sleep(10);
        ASSERT_TRUE(processor._lastFlushTime > flushTime2);
        ProcessedDocumentVecPtr docVec = processor.getProcessedDoc();
        ASSERT_EQ(4, docVec->size());
        ASSERT_EQ(1, processor._processErrorCounter->Get());
        ASSERT_EQ(14, processor._processDocCountCounter->Get());
        DocumentTestHelper::checkDocument(
                FakeDocument("3", ADD_DOC, false, true),
                (*docVec)[0]->getDocument());
        EXPECT_EQ(common::Locator(3), (*docVec)[0]->getLocator());
    }
    processor.stop();
    sleep(2);
}


TEST_F(BatchProcessorTest, testPooledDocumentProcessor)
{
    BatchProcessor processor("batch_size=5;max_enqueue_seconds=10");
    string configRoot = TEST_DATA_PATH"processor_test/config/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

    size_t docCount = 5;
    for (size_t i = 0; i < docCount; ++i) {
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
        processor.processDoc(rawDoc);
    }

    sleep(2);

    SingleDocProcessorChain* chain =
        dynamic_cast<SingleDocProcessorChain*>((*processor._chains)[0].get());
    ASSERT_TRUE(chain);
    const vector<DocumentProcessor*>& processors = chain->getDocumentProcessors();
    ReusedDocumentProcessor* pooledProcessor = NULL;
    for (auto processor : processors) {
        if (processor->getDocumentProcessorName() == "ReusedDocumentProcessor") {
            pooledProcessor = dynamic_cast<ReusedDocumentProcessor*>(processor);
        }
    }
    ASSERT_TRUE(pooledProcessor);

    ReusedDocumentProcessor* processor1 =
        dynamic_cast<ReusedDocumentProcessor*>(pooledProcessor->allocate());
    ReusedDocumentProcessor* processor2 =
        dynamic_cast<ReusedDocumentProcessor*>(pooledProcessor->allocate());

    int32_t counter1 = processor1->counter;
    int32_t counter2 = processor2->counter;
    processor1->deallocate();
    processor2->deallocate();

    // 5 docs, last doc will use first cloned chain
    ASSERT_EQ(1, counter1);
    ASSERT_EQ(4, counter2);
}

TEST_F(BatchProcessorTest, testMainSubProcessDoc) {
    string configRoot = TEST_DATA_PATH"processor_test/main_sub_config/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    BatchProcessor processor("batch_size=10");
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
    
    RawDocumentVecPtr rawDocs = createRawDocVecWithSubDocs(
            "1:true:1,2;"    // seeds=idx(1), chainId=1, processFail=true, subDoc:1,2
            "3:true:1,!2,3;" // seeds=idx(2), chainId=3, processFail=true, subDoc:1,!2(subProcessFail),3
            "2:true:1,2,3;"
            "2:false:1,2;"
            "2:true:2,3;"
            "3:false:1,2;"
            "1:true:!4,5;"
            "3:true:4,5,!6;"
            "1:false:1,2,4");
    
    for (size_t i = 0; i < rawDocs->size(); i++) {
        processor.processDoc((*rawDocs)[i]);
    }

    ProcessedDocumentVecPtr docs = processor.getProcessedDoc();
    ASSERT_TRUE(docs);
    checkProcessedDocs(docs,
                       "1:1:2;" // seeds=1 : chainId=1(-1 processFail,no indexDocument) : subDocCount=2
                       "2:3:2;"
                       "3:2:3;"
                       "3:1:3;"
                       "4:-1:0;"
                       "4:-1:0;"
                       "5:2:2;"
                       "5:1:2;"
                       "6:-1:0;"
                       "7:1:1;"
                       "8:3:2;"
                       "9:-1:0");
}

void BatchProcessorTest::checkProcessedDocs(
        const ProcessedDocumentVecPtr& docs, const string& pDocInfoStr)
{
    // key:chainId, value: hasSummary
    map<int, bool> chainMap;
    chainMap[1] = true;
    chainMap[2] = false;
    chainMap[3] = false;

    vector<vector<int>> pDocInfos;
    StringUtil::fromString(pDocInfoStr, pDocInfos, ":", ";");
    ASSERT_EQ(pDocInfos.size(), docs->size());

    for (size_t i = 0; i < pDocInfos.size(); i++) {
        common::Locator locator(pDocInfos[i][0]);
        EXPECT_EQ(locator, (*docs)[i]->getLocator());

        if (pDocInfos[i][1] == -1) {
            EXPECT_FALSE((*docs)[i]->getDocument());
        } else {
            IE_NAMESPACE(document)::NormalDocumentPtr doc =
                DYNAMIC_POINTER_CAST(IE_NAMESPACE(document)::NormalDocument,
                        (*docs)[i]->getDocument());
            EXPECT_TRUE(doc);
            DocumentTestHelper::checkDocument(
                    FakeDocument(StringUtil::toString(pDocInfos[i][0]),
                            ADD_DOC, chainMap[pDocInfos[i][1]], true),
                    (*docs)[i]->getDocument());

            ASSERT_EQ((size_t)pDocInfos[i][2], doc->GetSubDocuments().size());
        }
    }
}

RawDocumentVecPtr BatchProcessorTest::createRawDocVecWithSubDocs(const string &rawDocString)
{
    RawDocumentVecPtr rawDocs(new RawDocumentVec);
    vector<vector<string>> rawDocInfos;
    StringUtil::fromString(rawDocString, rawDocInfos, ":", ";");

    for (size_t i = 0; i < rawDocInfos.size(); i++) {
        assert(rawDocInfos[i].size() == 3);
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i + 1);
        rawDoc->setField("field", string("f") + rawDocInfos[i][0]);
        rawDoc->setField("section", string("s") + rawDocInfos[i][0]);
        if (rawDocInfos[i][1] == "false") {
            rawDoc->setField("need_process_fail", "true");
        }

        // set sub fields
        vector<string> subFields;
        StringUtil::fromString(rawDocInfos[i][2], subFields, ",");
        vector<string> needProcessFailFields;
        for (auto& field: subFields) {
            if (field[0] == '!') {
                field = field.substr(1);
                needProcessFailFields.push_back("true");
            } else {
                needProcessFailFields.push_back("false");
            }
        }
        rawDoc->setField("sub_title", StringUtil::toString(subFields, ""));
        rawDoc->setField("sub_id", StringUtil::toString(subFields, ""));
        rawDoc->setField("sub_need_process_fail", StringUtil::toString(needProcessFailFields, ""));
        rawDocs->push_back(rawDoc);
    }
    return rawDocs;
}


}
}
