#include "build_service/test/unittest.h"
#include "build_service/processor/Processor.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/RegionDocumentProcessor.h"
#include "build_service/processor/test/ProcessorTestUtil.h"
#include "build_service/processor/test/CustomizedDocParser.h"
#include "build_service/processor/test/SleepWorkItem.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include <fslib/fslib.h>
#include <autil/TimeUtility.h>
#include <autil/AtomicCounter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <beeper/beeper.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;
using namespace build_service::proto;

using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace processor {

class ProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    proto::PartitionId _partitionId;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};

void ProcessorTest::setUp() {
    _partitionId.Clear();
    _partitionId.mutable_buildid()->set_datatable("simple");
    *_partitionId.add_clusternames() = "simple_cluster";
    *_partitionId.add_clusternames() = "simple_no_summary_cluster";
    _counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
}

void ProcessorTest::tearDown() {
}

TEST_F(ProcessorTest, testStart) {
    // test MultiThreadProcessorWorkItemExecutor
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(2), processor._chains->size());

        ASSERT_TRUE(processor._executor);
        MultiThreadProcessorWorkItemExecutor* executor =
            dynamic_cast<MultiThreadProcessorWorkItemExecutor*>(processor._executor.get());
        ASSERT_TRUE(executor);
        EXPECT_EQ(size_t(3), executor->_processThreadPool->_threadPool.getThreadNum());
        EXPECT_EQ(size_t(250), executor->_processThreadPool->_queue.capacity());
        EXPECT_TRUE(executor->_processThreadPool->_running);
    }
    // test SingleThreadProcessorWorkItemExecutor
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(2), processor._chains->size());

        ASSERT_TRUE(processor._executor);
        SingleThreadProcessorWorkItemExecutor* executor =
            (SingleThreadProcessorWorkItemExecutor*)processor._executor.get();
        ASSERT_TRUE(executor); 
        EXPECT_TRUE(executor->_running); 
        EXPECT_EQ(size_t(250), executor->_queue->capacity()); 
        EXPECT_EQ(size_t(0), executor->_queue->size()); 
    }
}

TEST_F(ProcessorTest, testLoadChain) {
    Processor processor;
    {
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.loadChain(resourceReaderPtr, _partitionId, _counterMap));

        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(2), processor._chains->size());
    }
    {
        string configRoot = TEST_DATA_PATH"processor_test/update_config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.loadChain(resourceReaderPtr, _partitionId, _counterMap));

        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(1), processor._chains->size());
    }
}

TEST_F(ProcessorTest, testRewriteRegionConfig) {
    Processor processor;
    {
        string configRoot = TEST_DATA_PATH"processor_test/config_multi_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.loadChain(resourceReaderPtr, _partitionId, _counterMap));
        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(6), processor._chains->size());
        for (size_t i = 0; i < processor._chains->size(); ++i) {
            DocumentProcessorChainPtr chain = processor._chains->at(i);
            SingleDocProcessorChain* singleChain = ASSERT_CAST_AND_RETURN(
                SingleDocProcessorChain, chain.get());

            ASSERT_EQ(3u, singleChain->_documentProcessors.size());
            EXPECT_EQ(string("RegionDocumentProcessor"),
                      singleChain->_documentProcessors[0]->getDocumentProcessorName());

            RegionDocumentProcessor* regionProcessor = ASSERT_CAST_AND_RETURN(
                    RegionDocumentProcessor, singleChain->_documentProcessors[0]);

            EXPECT_EQ(i, regionProcessor->_specificRegionId);
            EXPECT_EQ(string(""), regionProcessor->_regionFieldName); 
            
            EXPECT_EQ(string("HashDocumentProcessor"),
                      singleChain->_documentProcessors[1]->getDocumentProcessorName());
            EXPECT_EQ(string("TokenizeDocumentProcessor"),
                      singleChain->_documentProcessors[2]->getDocumentProcessorName());
        }
    }
    {
        _partitionId.Clear();
        _partitionId.mutable_buildid()->set_datatable("simple1");
        *_partitionId.add_clusternames() = "simple_cluster";
        *_partitionId.add_clusternames() = "simple_no_summary_cluster";
        _counterMap.reset(new IE_NAMESPACE(util)::CounterMap()); 
        string configRoot = TEST_DATA_PATH"processor_test/config_multi_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.loadChain(resourceReaderPtr, _partitionId, _counterMap));
        EXPECT_TRUE(processor._chains);
        EXPECT_EQ(size_t(1), processor._chains->size());
        DocumentProcessorChainPtr chain = processor._chains->at(0);
        SingleDocProcessorChain* singleChain = ASSERT_CAST_AND_RETURN(
                SingleDocProcessorChain, chain.get());

        ASSERT_EQ(3u, singleChain->_documentProcessors.size());
        EXPECT_EQ(string("RegionDocumentProcessor"),
                  singleChain->_documentProcessors[0]->getDocumentProcessorName());
        EXPECT_EQ(string("HashDocumentProcessor"),
                  singleChain->_documentProcessors[1]->getDocumentProcessorName());
        EXPECT_EQ(string("TokenizeDocumentProcessor"),
                  singleChain->_documentProcessors[2]->getDocumentProcessorName());

        RegionDocumentProcessor* regionProcessor = ASSERT_CAST_AND_RETURN(
                RegionDocumentProcessor, singleChain->_documentProcessors[0]);

        EXPECT_EQ(INVALID_REGIONID, regionProcessor->_specificRegionId);
        EXPECT_EQ(string("region_field_test"), regionProcessor->_regionFieldName);         
    }    
}

TEST_F(ProcessorTest, testLoadOneCluster) {
    Processor processor;
    string configRoot = TEST_DATA_PATH"processor_test/config/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    proto::PartitionId partitionId;
    partitionId.mutable_buildid()->set_datatable("simple");
    *partitionId.add_clusternames() = "simple_cluster";
    ASSERT_TRUE(processor.loadChain(resourceReaderPtr, partitionId, _counterMap));

    EXPECT_TRUE(processor._chains);
    EXPECT_EQ(size_t(1), processor._chains->size());
}


TEST_F(ProcessorTest, testProcessDocTracer) {
    string beeperConfig = R"({
    "recorder_configs" :   
    [
        {
            "id" : "doc_trace",     
            "type" : "sqi",
            "parameters" :
            [
                {
                    "key" : "service",
                    "value" : "build_service_test"
                },
                { 
                   "key" : "host" ,
                   "value":"monitor.tisplus.alibaba.net"
                },
                { 
                   "key" : "apiUser" ,
                   "value":"bsdaily"
                },
                { 
                   "key" : "apiToken" ,
                   "value":"bsdaily"
                }
            ]
        }
    ],
    "level_configs" :       
    [
        {
            "level" : "default",
            "report_targets" :
            [
                {
                    "sync_mode" : true,
                    "recorder_id" : "doc_trace"
                }
            ]
        }
    ]
})";
    beeper::EventTags globalTags;
    globalTags.AddTag("generation", "111111");
    BEEPER_INIT_FROM_CONF_STRING(beeperConfig, globalTags);
    DECLARE_BEEPER_COLLECTOR(IE_DOC_TRACER_COLLECTOR_NAME);
    Processor processor;
    string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
    
    size_t docCount = 2;
    for (size_t i = 0; i < docCount; ++i) {
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
        rawDoc->setField(BUILTIN_KEY_TRACE_ID, StringUtil::toString(i));
        processor.processDoc(rawDoc);
    }
    for (size_t i = 0; i < docCount; ++i) {
        DocumentTestHelper::checkDocument(i, processor.getProcessedDoc());
    }
}


TEST_F(ProcessorTest, testProcessDoc) {
    // test MultiThread process
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        size_t docCount = 100;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        for (size_t i = 0; i < docCount; ++i) {
            DocumentTestHelper::checkDocument(i, processor.getProcessedDoc());
        }

        // test no memory leak after stop
        processor.stop();
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docCount);
        processor.processDoc(rawDoc);
    }
    // test SingleThread process
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        size_t docCount = 100;
        for (size_t i = 0; i < docCount; ++i) {
            RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
            processor.processDoc(rawDoc);
        }
        for (size_t i = 0; i < docCount; ++i) {
            DocumentTestHelper::checkDocument(i, processor.getProcessedDoc());
        }
        processor.stop();
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docCount);
        processor.processDoc(rawDoc);        
    }    
}

TEST_F(ProcessorTest, testSchemaUpdatableCluster) {
    string configRoot = TEST_DATA_PATH"processor_test/config_schema_updatable/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    proto::PartitionId pid;
    pid.mutable_buildid()->set_appname("app_name");
    pid.mutable_buildid()->set_generationid(1);
    pid.mutable_buildid()->set_datatable("simple");
    pid.set_role(ROLE_PROCESSOR);
    pid.set_step(BUILD_STEP_INC);
    *pid.mutable_taskid() = "1";

    {
        // schema updatable cluster
        Processor processor;
        pid.mutable_range()->set_from(0);
        pid.mutable_range()->set_to(32767);
        pid.add_clusternames("simple2_cluster");
        ASSERT_TRUE(processor.start(resourceReaderPtr, pid, NULL, _counterMap));

        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(0);
        processor.processDoc(rawDoc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1, processedDocVec->size());
        ProcessedDocument::DocClusterMetaVec docClusterMeta = (*processedDocVec)[0]->_docClusterMeta;
        ASSERT_EQ(1, docClusterMeta.size());
        ASSERT_EQ("simple2_cluster", docClusterMeta[0].clusterName);
        
    }
    {
        // default processor group
        Processor processor;
        pid.mutable_range()->set_from(0);
        pid.mutable_range()->set_to(32767);
        pid.clear_clusternames();
        pid.add_clusternames("simple_cluster");
        pid.add_clusternames("simple3_cluster");
        ASSERT_TRUE(processor.start(resourceReaderPtr, pid, NULL, _counterMap));

        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(0);
        processor.processDoc(rawDoc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1, processedDocVec->size());
        ProcessedDocument::DocClusterMetaVec docClusterMeta = (*processedDocVec)[0]->_docClusterMeta;
        ASSERT_EQ(2, docClusterMeta.size());
        ASSERT_EQ("simple_cluster", docClusterMeta[0].clusterName);
        ASSERT_EQ("simple3_cluster", docClusterMeta[1].clusterName);
        
    }
}

TEST_F(ProcessorTest, testProcessDocInTwoRegions) {
    auto checkProcessedDoc = [] (
            const ProcessedDocumentPtr& processedDoc,
            int pkey, int skey, regionid_t regionid)
    {
        ASSERT_EQ(1, processedDoc->_docClusterMeta.size());
        ASSERT_EQ(hashid_t(pkey), processedDoc->_docClusterMeta[0].hashId);
        IE_NAMESPACE(document)::NormalDocumentPtr indexDoc = DYNAMIC_POINTER_CAST(
                IE_NAMESPACE(document)::NormalDocument, processedDoc->_document);
        ASSERT_TRUE(indexDoc);
        ASSERT_EQ(regionid, indexDoc->GetRegionId());
        const auto& kvIndexDoc = indexDoc->GetKVIndexDocument();
        ASSERT_EQ((uint64_t)pkey, kvIndexDoc->GetPkeyHash());
        ASSERT_EQ((uint64_t)skey, kvIndexDoc->GetSkeyHash());        
    };
    {   // test dispatch_to_all_region
        Processor processor;
        proto::PartitionId partitionId;
        partitionId.Clear();
        partitionId.mutable_buildid()->set_datatable("simple");
        *partitionId.add_clusternames() = "simple_cluster";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
        string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, partitionId, NULL, counterMap));

        RawDocumentPtr doc = ProcessorTestUtil::createRawDocument("nid:1;friendid:100;timestamp:0");
        processor.processDoc(doc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(2u, processedDocVec->size());

        checkProcessedDoc((*processedDocVec)[0], 1, 100, 0);
        checkProcessedDoc((*processedDocVec)[1], 100, 1, 1);            
    }
    {   // test dispatch_by_field
        Processor processor;
        proto::PartitionId partitionId;
        partitionId.Clear();
        partitionId.mutable_buildid()->set_datatable("simple1");
        *partitionId.add_clusternames() = "simple_cluster";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
        string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, partitionId, NULL, counterMap));

        RawDocumentPtr doc = ProcessorTestUtil::createRawDocument(
                "nid:1;friendid:100;timestamp:0;region_field:region2");
        processor.processDoc(doc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 100, 1, 1);

        doc = ProcessorTestUtil::createRawDocument(
                "nid:2;friendid:200;timestamp:1;region_field:region1");
        processor.processDoc(doc);
        processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 2, 200, 0);

        // test invalid regionName
        doc = ProcessorTestUtil::createRawDocument(
                "nid:2;friendid:200;timestamp:1;region_field:region3");
        processor.processDoc(doc);
        processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());        
        ASSERT_EQ(0, (*processedDocVec)[0]->_docClusterMeta.size());        
    }
    {   // test dispatch_by_id
        Processor processor;
        proto::PartitionId partitionId;
        partitionId.Clear();
        partitionId.mutable_buildid()->set_datatable("simple2");
        *partitionId.add_clusternames() = "simple_cluster";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
        string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, partitionId, NULL, counterMap));

        // ha_reserved_region_field is useless, because dispatch_by_id(regionid=0)
        RawDocumentPtr doc = ProcessorTestUtil::createRawDocument(
                "nid:1;friendid:100;timestamp:0;ha_reserved_region_field:region2");
        processor.processDoc(doc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 1, 100, 0);

        doc = ProcessorTestUtil::createRawDocument(
                "nid:2;friendid:200;timestamp:1;ha_reserved_region_field:region3");
        processor.processDoc(doc);
        processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 2, 200, 0);        
    }
    {   // test default config : dispatch_by_field, default region field = ha_reserved_region_field
        Processor processor;
        proto::PartitionId partitionId;
        partitionId.Clear();
        partitionId.mutable_buildid()->set_datatable("simple3");
        *partitionId.add_clusternames() = "simple_cluster";
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap()); 
        string configRoot = TEST_DATA_PATH"processor_test/config_two_region/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, partitionId, NULL, counterMap));

        RawDocumentPtr doc = ProcessorTestUtil::createRawDocument(
                "nid:1;friendid:100;timestamp:0;ha_reserved_region_field:region2");
        processor.processDoc(doc);
        ProcessedDocumentVecPtr processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 100, 1, 1);

        doc = ProcessorTestUtil::createRawDocument(
                "nid:2;friendid:200;timestamp:1;ha_reserved_region_field:region1");
        processor.processDoc(doc);
        processedDocVec = processor.getProcessedDoc();
        ASSERT_EQ(1u, processedDocVec->size());
        checkProcessedDoc((*processedDocVec)[0], 2, 200, 0); 
    }            
}


TEST_F(ProcessorTest, testStop) {
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        AtomicCounter processedCount;
        int workItemCount = 10;
        MultiThreadProcessorWorkItemExecutor* executor =
            dynamic_cast<MultiThreadProcessorWorkItemExecutor*>(processor._executor.get());
        ASSERT_TRUE(executor);
        
        for (int i = 0; i < workItemCount; i++) {
            executor->push(new SleepWorkItem(3, processedCount));
        }
        processor.stop();
        EXPECT_EQ(workItemCount, processedCount.getValue());
        // check get doc after stop
        for (int i = 0; i < workItemCount; ++i) {
            EXPECT_TRUE(processor.getProcessedDoc());
        }
        EXPECT_TRUE(!processor.getProcessedDoc());
    }
    {
        Processor processor;
        string configRoot = TEST_DATA_PATH"processor_test/config_single_thread/";
        ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
        ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

        SingleThreadProcessorWorkItemExecutor* executor =
            dynamic_cast<SingleThreadProcessorWorkItemExecutor*>(processor._executor.get());
        ASSERT_TRUE(executor);

        AtomicCounter processedCount;
        int workItemCount = 10;
        for (int i = 0; i < workItemCount; i++) {
            executor->push(new SleepWorkItem(3, processedCount));
        }
        processor.stop();
        EXPECT_EQ(workItemCount, processedCount.getValue());
        // check get doc after stop
        for (int i = 0; i < workItemCount; ++i) {
            EXPECT_TRUE(processor.getProcessedDoc());
        }
        EXPECT_TRUE(!processor.getProcessedDoc());
    }    
}

TEST_F(ProcessorTest, testCustomizedDocParser) {
    Processor processor;
    string configRoot = TEST_DATA_PATH"processor_test/customized_doc_parser_config/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    *_partitionId.add_clusternames() = "simple_cluster2";
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));

    size_t docCount = 10;
    for (size_t i = 0; i < docCount; ++i) {
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
        stringstream ss;
        ss << i * 10 << "";
        rawDoc->setField("CMD", "add");
        rawDoc->setField("myId", ss.str());
        rawDoc->setField("price2", ss.str());
        processor.processDoc(rawDoc);
    }
    for (size_t i = 0; i < docCount; ++i) {
        document::ProcessedDocumentVecPtr result = processor.getProcessedDoc();
        //chain0
        MyDocumentPtr doc0 = DYNAMIC_POINTER_CAST(MyDocument, (*result)[0]->getDocument());
        ASSERT_EQ(i * 10, doc0->GetDocId());
        ASSERT_EQ(i * 10, doc0->GetData());
        ASSERT_EQ(10, doc0->GetSerializedVersion());
        //chain1
        NormalDocumentPtr doc1 = DYNAMIC_POINTER_CAST(NormalDocument, (*result)[1]->getDocument());
        ASSERT_TRUE(doc1);
        ASSERT_TRUE(doc1->GetIndexDocument());
        ASSERT_EQ(-1, doc1->GetDocId());
        ASSERT_EQ(DocOperateType::ADD_DOC, doc1->GetDocOperateType());
        //chain2
        MyDocumentPtr doc2 = DYNAMIC_POINTER_CAST(MyDocument, (*result)[2]->getDocument());
        ASSERT_EQ(i * 10, doc2->GetDocId());
        ASSERT_EQ(i * 10, doc2->GetData());
        ASSERT_EQ(7, doc2->GetSerializedVersion());
    }
    processor.stop();    
}

TEST_F(ProcessorTest, testSlowUpdate) {
    Processor processor;
    string baseConfigRoot = TEST_DATA_PATH "processor_test/config_slow_update/";
    string configPath = GET_TEST_DATA_PATH() + "/config_slow_update/";
    ASSERT_EQ(EC_OK, FileSystem::copy(baseConfigRoot, configPath));
    string dataTableStr = R"(
{
    "processor_chain_config" : [
        {
            "clusters" : [
                "simple_cluster"
            ],
            "document_processor_chain" : [
                {
                    "class_name" : "TokenizeDocumentProcessor",
                    "module_name" : "",
                    "parameters" : {
                    }
                },                
                {
                    "class_name" : "SlowUpdateProcessor",
                    "module_name" : "",
                    "parameters" : {
                        "sync_interval" : "1",
                        "config_path" : "$CONFIG_PATH"
                    }
                }
            ],
            "modules" : [
            ]
        }
    ],
    "processor_config" : {
        "processor_queue_size" : 250,
        "processor_thread_num" : 3
    },
    "processor_rule_config" : {
        "partition_resource" : 100,
        "processor_count" : 10
    }
}    
)";
    
    auto content =
        dataTableStr.replace(dataTableStr.find("$CONFIG_PATH"), 12, configPath + "conf.json");
    ASSERT_EQ(EC_OK,
              FileSystem::writeFile(configPath + "data_tables/simple_table.json", content));

    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configPath));
    *_partitionId.add_clusternames() = "simple_cluster";
    ASSERT_TRUE(processor.start(resourceReaderPtr, _partitionId, NULL, _counterMap));
    sleep(1);
    size_t docCount = 10;
    for (size_t i = 0; i < docCount; ++i) {
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i);
        rawDoc->setDocTimestamp(i);
        processor.processDoc(rawDoc);
    }
    for (size_t i = 0; i < 5; ++i) {
        document::ProcessedDocumentVecPtr result = processor.getProcessedDoc();
        const auto& meta = (*result)[0]->getDocClusterMetaVec();
        EXPECT_EQ(1, meta[0].buildType);
    }
    for (size_t i = 5; i < 10; ++i) {
        document::ProcessedDocumentVecPtr result = processor.getProcessedDoc();
        const auto& meta = (*result)[0]->getDocClusterMetaVec();
        EXPECT_EQ(0, meta[0].buildType);
    }
    processor.stop();    
}

TEST_F(ProcessorTest, testProcessorWithChainSelector) 
{
    Processor processor;
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
    
}
}
