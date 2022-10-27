#include "build_service/test/unittest.h"
#include "build_service/processor/SelectBuildTypeProcessor.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;

IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

class SelectBuildTypeProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    SelectBuildTypeProcessorTest();
    ~SelectBuildTypeProcessorTest() {}
public:
    void setUp() {}
    void tearDown() {}

    bool checkClusterMetas(const ProcessedDocument::DocClusterMetaVec &actualMetas,
                                 const uint8_t &expectBuildType) {
        for (size_t i = 0; i < actualMetas.size(); ++i) {
            if (actualMetas[i].buildType == expectBuildType) {
                return true;
            }
        }
        return false;
    }
private:
    vector<string> _clusterNames;
    ProcessedDocument::DocClusterMetaVec _clusterMetas;
};

SelectBuildTypeProcessorTest::SelectBuildTypeProcessorTest() {
    _clusterNames.push_back("cluster1");
    _clusterNames.push_back("cluster1");

    for (size_t i = 0; i < _clusterNames.size(); ++i) {
        ProcessedDocument::DocClusterMeta meta;
        meta.clusterName = _clusterNames[i];
        _clusterMetas.push_back(meta);
    }
}

TEST_F(SelectBuildTypeProcessorTest, testCopyConstruct) {
    SelectBuildTypeProcessor processor;
    DocProcessorInitParam param;
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "realtime_build";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "false";
    processor.init(param);
    SelectBuildTypeProcessor processorCopy(processor);
    EXPECT_EQ(processor._notRealtimeKey, processorCopy._notRealtimeKey);
    EXPECT_EQ(processor._notRealtimeValue, processorCopy._notRealtimeValue);
}

TEST_F(SelectBuildTypeProcessorTest, testInitSuccess) {
    ExtendDocumentPtr document(new ExtendDocument);
    DocProcessorInitParam param;
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "realtime_build";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "false";
    SelectBuildTypeProcessor processor;
    param.clusterNames = _clusterNames;
    ASSERT_TRUE(processor.init(param));
}

TEST_F(SelectBuildTypeProcessorTest, testInitFail) {
    {
    ExtendDocumentPtr document(new ExtendDocument);
    DocProcessorInitParam param;
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "realtime_build";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "";
    SelectBuildTypeProcessor processor;
    param.clusterNames = _clusterNames;
    ASSERT_FALSE(processor.init(param));
    }
    {
    ExtendDocumentPtr document(new ExtendDocument);
    DocProcessorInitParam param;
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "false";
    SelectBuildTypeProcessor processor;
    param.clusterNames = _clusterNames;
    ASSERT_FALSE(processor.init(param));
    }
}

TEST_F(SelectBuildTypeProcessorTest, testUndefinedRealtimeFieldName) {
    ExtendDocumentPtr document(new ExtendDocument);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    SelectBuildTypeProcessor processor;
    DocProcessorInitParam param;
    param.clusterNames = _clusterNames;
    ASSERT_FALSE(processor.init(param));
}


TEST_F(SelectBuildTypeProcessorTest, testRealtimeBuild) {
    ExtendDocumentPtr document(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("realtime_build", "true");
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    SelectBuildTypeProcessor processor;
    DocProcessorInitParam param;
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "realtime_build";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "false";
    param.clusterNames = _clusterNames;
    ASSERT_TRUE(processor.init(param));
    ASSERT_TRUE(processor.process(document));
    const ProcessedDocument::DocClusterMetaVec &actualMetas = 
        document->getProcessedDocument()->getDocClusterMetaVec();
    ASSERT_EQ((size_t)2, actualMetas.size());
    EXPECT_TRUE(checkClusterMetas(actualMetas, 0));
}

TEST_F(SelectBuildTypeProcessorTest, testNotRealtimeBuild) {
    ExtendDocumentPtr document(new ExtendDocument);
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setField("realtime_build", "false");
    document->setRawDocument(rawDoc);
    document->getProcessedDocument()->setDocClusterMetaVec(_clusterMetas);
    SelectBuildTypeProcessor processor;
    DocProcessorInitParam param;
    param.parameters[SelectBuildTypeProcessor::REALTIME_KEY_NAME] = "realtime_build";
    param.parameters[SelectBuildTypeProcessor::REALTIME_VALUE_NAME] = "false";
    param.clusterNames = _clusterNames;
    ASSERT_TRUE(processor.init(param));
    ASSERT_TRUE(processor.process(document));
    const ProcessedDocument::DocClusterMetaVec &actualMetas = 
        document->getProcessedDocument()->getDocClusterMetaVec();
    ASSERT_EQ((size_t)2, actualMetas.size());
    EXPECT_TRUE(checkClusterMetas(actualMetas, 1));
}


}
}
