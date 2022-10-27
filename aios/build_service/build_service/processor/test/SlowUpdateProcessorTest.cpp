#include "build_service/test/unittest.h"
#include "build_service/processor/SlowUpdateProcessor.h"
#include <indexlib/document/raw_document/default_raw_document.h>
#include "build_service/processor/BuildInDocProcessorFactory.h"
#include "build_service/util/FileUtil.h"

using namespace std;
using namespace testing;

using namespace build_service::document;
using namespace build_service::util;

namespace build_service {
namespace processor {

class SlowUpdateProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp() {
        _clusterNames.push_back("cluster1");
        _clusterNames.push_back("cluster1");

        for (size_t i = 0; i < _clusterNames.size(); ++i) {
            ProcessedDocument::DocClusterMeta meta;
            meta.clusterName = _clusterNames[i];
            _clusterMetas.push_back(meta);
        }
        _slowUpdateConfigPath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "config");
        _param = DocProcessorInitParam();
        _param.parameters[SlowUpdateProcessor::CONFIG_PATH] = _slowUpdateConfigPath;
        _param.parameters[SlowUpdateProcessor::SYNC_INTERVAL] = "1";
        _param.clusterNames = _clusterNames;
    }
    void tearDown() {}

private: 
    void removeSlowUpdateConfig();
    void updateSlowUpdateConfig(int64_t timestamp, const std::string &filterKey,
                                const std::string &filterValue,
                                SlowUpdateProcessor::ActionType action =
                                    SlowUpdateProcessor::ActionType::SKIP_REALTIME) {
        updateSlowUpdateConfig(_slowUpdateConfigPath, timestamp, filterKey, filterValue,
                               action);
    }
    DocProcessorInitParam updateSlowUpdateConfig(
        const std::string &configPath, int64_t timestamp, const std::string &filterKey,
        const std::string &filterValue,
        SlowUpdateProcessor::ActionType action =
            SlowUpdateProcessor::ActionType::SKIP_REALTIME);

private:
    vector<string> _clusterNames;
    ProcessedDocument::DocClusterMetaVec _clusterMetas;

    std::string _slowUpdateConfigPath;
    DocProcessorInitParam _param;
};
void SlowUpdateProcessorTest::removeSlowUpdateConfig() {
    FileUtil::remove(_slowUpdateConfigPath);
}

DocProcessorInitParam SlowUpdateProcessorTest::updateSlowUpdateConfig(
    const std::string &configPath, int64_t timestamp, const std::string &filterKey,
    const std::string &filterValue, SlowUpdateProcessor::ActionType action) {
    SlowUpdateProcessor::Config conf;
    conf.timestamp = timestamp;
    conf.filterKey = filterKey;
    conf.filterValue = filterValue;
    conf.action = action;

    string content = ToJsonString(conf);
    FileUtil::remove(configPath);
    auto ret = FileUtil::writeFile(configPath, content);
    assert(ret);
    DocProcessorInitParam param = _param;
    param.parameters[SlowUpdateProcessor::CONFIG_PATH] = configPath;
    return param;
}

TEST_F(SlowUpdateProcessorTest, testFactory) {
    BuildInDocProcessorFactory factory;
    KeyValueMap param;
    ASSERT_TRUE(factory.init(param));
    auto processor = factory.createDocumentProcessor(SlowUpdateProcessor::PROCESSOR_NAME);
    ASSERT_TRUE(processor);
    auto typedProcessor = dynamic_cast<SlowUpdateProcessor *>(processor);
    ASSERT_TRUE(typedProcessor);
    delete processor;
}

TEST_F(SlowUpdateProcessorTest, testInit) {
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp":"x12346",
            "filter_key": "source",
            "filter_value": "dump"
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(-1, processor._config.timestamp);
    }
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": "dump"
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(12346, processor._config.timestamp);
        ASSERT_EQ(string("source"), processor._config.filterKey);
        ASSERT_EQ(string("dump"), processor._config.filterValue);
        ASSERT_EQ(SlowUpdateProcessor::ActionType::SKIP_REALTIME, processor._config.action);
    }
    {
        SlowUpdateProcessor processor(false);
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(-1, processor._config.timestamp);
    }
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source"
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(-1, processor._config.timestamp);
    }
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": "",
            "action": "skip_all"
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));        
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(12346, processor._config.timestamp);
        ASSERT_EQ(string("source"), processor._config.filterKey);
        ASSERT_EQ(string(""), processor._config.filterValue);
        ASSERT_EQ(SlowUpdateProcessor::ActionType::SKIP_ALL, processor._config.action);
    }
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": "",
            "action": "skip_xall"
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));        
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(-1, processor._config.timestamp);
    }
    {
        SlowUpdateProcessor processor;
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": ""
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        _param.parameters[SlowUpdateProcessor::SYNC_INTERVAL] = "abc";
        ASSERT_FALSE(processor.init(_param));
    }
    {
        SlowUpdateProcessor processor;
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": ""
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        _param.parameters[SlowUpdateProcessor::SYNC_INTERVAL] = "abc";
        ASSERT_FALSE(processor.init(_param));
    }
    {
        SlowUpdateProcessor processor(false);
        string conf = R"(
        {
            "timestamp": 12346,
            "filter_key": "source",
            "filter_value": ""
        }
        )";
        FileUtil::remove(_slowUpdateConfigPath);
        ASSERT_TRUE(FileUtil::writeFile(_slowUpdateConfigPath, conf));
        _param.parameters[SlowUpdateProcessor::CONFIG_PATH] = "abcdefg";
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();
        ASSERT_EQ(-1, processor._config.timestamp);
    }
}

TEST_F(SlowUpdateProcessorTest, testAllocate) {
    {
        SlowUpdateProcessor processor(false);
        updateSlowUpdateConfig(12346, "source", "dump",
                               SlowUpdateProcessor::ActionType::SKIP_ALL);
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();

        auto executor = processor.allocate();
        ASSERT_TRUE(executor);
        EXPECT_EQ(12346, executor->_config.timestamp);
        EXPECT_EQ(string("source"), executor->_config.filterKey);
        EXPECT_EQ(string("dump"), executor->_config.filterValue);
        EXPECT_EQ(SlowUpdateProcessor::ActionType::SKIP_ALL, executor->_config.action);
        delete executor;
    }
    {
        SlowUpdateProcessor processor(false);
        updateSlowUpdateConfig(12346, "source", "dump");
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();        

        auto executor = processor.allocate();
        ASSERT_TRUE(executor);
        EXPECT_EQ(12346, executor->_config.timestamp);
        EXPECT_EQ(string("source"), executor->_config.filterKey);
        EXPECT_EQ(string("dump"), executor->_config.filterValue);
        EXPECT_EQ(SlowUpdateProcessor::ActionType::SKIP_REALTIME, executor->_config.action);
        delete executor;
    }
    {
        SlowUpdateProcessor processor(false);
        updateSlowUpdateConfig(12346, "source", "dump", SlowUpdateProcessor::ActionType::UNKNOWN);
        ASSERT_TRUE(processor.init(_param));
        processor.syncConfig();        

        auto executor = processor.allocate();
        ASSERT_TRUE(executor);
        EXPECT_EQ(-1, executor->_config.timestamp);
        EXPECT_EQ(string(""), executor->_config.filterKey);
        EXPECT_EQ(string(""), executor->_config.filterValue);
        EXPECT_EQ(SlowUpdateProcessor::ActionType::UNKNOWN, executor->_config.action);
        delete executor;
    }    
}

TEST_F(SlowUpdateProcessorTest, testSimple) {
    ExtendDocumentPtr document(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("source", "dump");
    rawDoc->setDocTimestamp(12345);
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    SlowUpdateProcessor processor;
    updateSlowUpdateConfig(12346, "source", "dump");
    ASSERT_TRUE(processor.init(_param));
    sleep(1);
    ASSERT_TRUE(processor.process(document));
    const ProcessedDocument::DocClusterMetaVec &actualMetas = 
        document->getProcessedDocument()->getDocClusterMetaVec();
    ASSERT_EQ((size_t)2, actualMetas.size());
    EXPECT_EQ((uint8_t)1, actualMetas[0].buildType);
    EXPECT_EQ((uint8_t)1, actualMetas[1].buildType);

    document.reset(new ExtendDocument);
    rawDoc.reset(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("source", "dump");
    rawDoc->setDocTimestamp(12346);
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    ASSERT_TRUE(processor.process(document));
    const ProcessedDocument::DocClusterMetaVec &actualMetas2 = 
        document->getProcessedDocument()->getDocClusterMetaVec();
    ASSERT_EQ((size_t)2, actualMetas2.size());
    EXPECT_EQ((uint8_t)0, actualMetas2[0].buildType);
    EXPECT_EQ((uint8_t)0, actualMetas2[1].buildType);

    document.reset(new ExtendDocument);
    rawDoc.reset(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("source", "pora");
    rawDoc->setDocTimestamp(12345);
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    ASSERT_TRUE(processor.process(document));
    const ProcessedDocument::DocClusterMetaVec &actualMetas3 = 
        document->getProcessedDocument()->getDocClusterMetaVec();
    ASSERT_EQ((size_t)2, actualMetas3.size());
    EXPECT_EQ((uint8_t)0, actualMetas3[0].buildType);
    EXPECT_EQ((uint8_t)0, actualMetas3[1].buildType);

    updateSlowUpdateConfig(12346, "source", "pora", SlowUpdateProcessor::ActionType::SKIP_ALL);
    sleep(1);
    document.reset(new ExtendDocument);
    rawDoc.reset(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("source", "pora");
    rawDoc->setDocTimestamp(12345);
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    ASSERT_TRUE(processor.process(document));
    EXPECT_EQ(SKIP_DOC, rawDoc->getDocOperateType());
}

TEST_F(SlowUpdateProcessorTest, testSkip) {
    SlowUpdateProcessor processor;
    ASSERT_TRUE(processor.init(_param));
    updateSlowUpdateConfig(12346, "source", "pora", SlowUpdateProcessor::ActionType::SKIP_ALL);
    sleep(1);
    {
        ExtendDocumentPtr document(new ExtendDocument);
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("source", "pora");
        rawDoc->setDocTimestamp(12345);
        rawDoc->setDocOperateType(UPDATE_FIELD);
        document->setRawDocument(rawDoc);
        document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
        ASSERT_TRUE(processor.process(document));
        EXPECT_EQ(SKIP_DOC, rawDoc->getDocOperateType());
    }
}

TEST_F(SlowUpdateProcessorTest, testChainedProcessor) {
    SlowUpdateProcessor processor0;
    auto param =
        updateSlowUpdateConfig(_slowUpdateConfigPath + "/config0", 12346, "source", "dump0");    
    ASSERT_TRUE(processor0.init(param));

    SlowUpdateProcessor processor1;
    param =
        updateSlowUpdateConfig(_slowUpdateConfigPath + "/config1", 12346, "source", "dump1");        
    ASSERT_TRUE(processor1.init(param));

    SlowUpdateProcessor processor2;
    param =
        updateSlowUpdateConfig(_slowUpdateConfigPath + "/config2", 12346, "source", "pora", SlowUpdateProcessor::ActionType::SKIP_ALL);
    ASSERT_TRUE(processor2.init(param));

    sleep(1);

    {
        ExtendDocumentPtr document(new ExtendDocument);
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("source", "dump0");
        rawDoc->setDocTimestamp(12345);
        document->setRawDocument(rawDoc);
        document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
        ASSERT_TRUE(processor0.process(document));
        ASSERT_TRUE(processor1.process(document));        
        const auto &metas = document->getProcessedDocument()->getDocClusterMetaVec();
        ASSERT_EQ(2u, metas.size());
        EXPECT_EQ((uint8_t)1, metas[0].buildType);
        EXPECT_EQ((uint8_t)1, metas[1].buildType);        
    }
    {
        ExtendDocumentPtr document(new ExtendDocument);
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("source", "dump1");
        rawDoc->setDocTimestamp(12345);
        document->setRawDocument(rawDoc);
        document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
        ASSERT_TRUE(processor0.process(document));
        ASSERT_TRUE(processor1.process(document));        
        const auto &metas = document->getProcessedDocument()->getDocClusterMetaVec();
        ASSERT_EQ(2u, metas.size());
        EXPECT_EQ((uint8_t)1, metas[0].buildType);
        EXPECT_EQ((uint8_t)1, metas[1].buildType);        
    }
    {
        ExtendDocumentPtr document(new ExtendDocument);
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("source", "pora");
        rawDoc->setDocTimestamp(12345);
        document->setRawDocument(rawDoc);
        document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
        ASSERT_TRUE(processor0.process(document));
        ASSERT_TRUE(processor1.process(document));        
        const auto &metas = document->getProcessedDocument()->getDocClusterMetaVec();
        ASSERT_EQ(2u, metas.size());
        EXPECT_EQ((uint8_t)0, metas[0].buildType);
        EXPECT_EQ((uint8_t)0, metas[1].buildType);        
    }
    {
        ExtendDocumentPtr document(new ExtendDocument);
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        rawDoc->setField("source", "pora");
        rawDoc->setDocTimestamp(12345);
        document->setRawDocument(rawDoc);
        document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
        ASSERT_TRUE(processor0.process(document));
        ASSERT_TRUE(processor1.process(document));
        ASSERT_TRUE(processor2.process(document));
        EXPECT_EQ(SKIP_DOC, rawDoc->getDocOperateType());
    }
}

}
}
