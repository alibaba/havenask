#include "build_service/test/unittest.h"
#include "build_service/processor/DistributeDocumentProcessor.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;
using namespace testing;

IE_NAMESPACE_USE(config);
using namespace build_service::document;

namespace build_service {
namespace processor {

#define checkInit(kvMap, ret)                   \
    DistributeDocumentProcessor processor;      \
    do {                                        \
        DocProcessorInitParam param;            \
        param.parameters = kvMap;               \
        param.clusterNames = _clusterNames;      \
        ASSERT_EQ(ret, processor.init(param));   \
    } while (0)                                  \

#define checkInitFail(kvMap)                                            \
    do {                                                                \
        checkInit(kvMap, false);                                        \
    } while (0)                                                         \

class DistributeDocumentProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    DistributeDocumentProcessorTest();
    ~DistributeDocumentProcessorTest() {}
public:
    void setUp() {}
    void tearDown() {}

    static string getClusterStr(const ExtendDocumentPtr &doc) {
        const ProcessedDocument::DocClusterMetaVec &actualMetas =
            doc->getProcessedDocument()->getDocClusterMetaVec();
        vector<string> clusters;
        for (size_t i = 0; i < actualMetas.size(); ++i) {
            clusters.push_back(actualMetas[i].clusterName);
        }
        return StringUtil::toString(clusters, ",");
    }

    ExtendDocumentPtr createDocument(const string& distFieldName,
        const string &distFieldValue, const string &clustersStr)
    {
        RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
        ExtendDocumentPtr document(new ExtendDocument);
        rawDoc->setField(distFieldName, distFieldValue);
        document->setRawDocument(rawDoc);
        ProcessedDocument::DocClusterMetaVec clusterMetas;
        vector<string> clustersVec = StringTokenizer::tokenize(ConstString(clustersStr), ",");
        for (size_t i = 0; i < clustersVec.size(); i++) {
            ProcessedDocument::DocClusterMeta meta;
            meta.clusterName = clustersVec[i];
            clusterMetas.push_back(meta);
        }
        document->getProcessedDocument()->setDocClusterMetaVec(clusterMetas);
        return document;
    }

    static void processDoc(const KeyValueMap &kvMap,
            const vector<string> clusters, const ExtendDocumentPtr &doc)
    {
        DistributeDocumentProcessor processor;
        DocProcessorInitParam param;
        param.parameters = kvMap;
        param.clusterNames = clusters;
        ASSERT_TRUE(processor.init(param));
        ASSERT_TRUE(processor.process(doc));
    }

private:
    KeyValueMap _kvWithDefault;
    KeyValueMap _kvWithoutDefault;
    vector<string> _clusterNames;
    ProcessedDocument::DocClusterMetaVec _clusterMetas;
};

DistributeDocumentProcessorTest::DistributeDocumentProcessorTest() {
    _kvWithoutDefault[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "distribute";
    _kvWithoutDefault[DistributeDocumentProcessor::DISTRIBUTE_RULE] = "1: cluster1,cluster2; 2: cluster3";

    _kvWithDefault[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "distribute";
    _kvWithDefault[DistributeDocumentProcessor::DISTRIBUTE_RULE] = "1: cluster1,cluster2; 2: cluster3";
    _kvWithDefault[DistributeDocumentProcessor::DEFAULT_CLUSTERS] = "cluster4, cluster5";

    _clusterNames.push_back("cluster1");
    _clusterNames.push_back("cluster2");
    _clusterNames.push_back("cluster3");
    _clusterNames.push_back("cluster4");
    _clusterNames.push_back("cluster5");

    for (size_t i = 0; i < _clusterNames.size(); ++i) {
        ProcessedDocument::DocClusterMeta meta;
        meta.clusterName = _clusterNames[i];
        _clusterMetas.push_back(meta);
    }
}

TEST_F(DistributeDocumentProcessorTest, testCopyConstruct) {
    DistributeDocumentProcessor processor;
    DocProcessorInitParam param;
    param.parameters = _kvWithDefault;
    param.clusterNames = _clusterNames;
    ASSERT_TRUE(processor.init(param));
    DistributeDocumentProcessor processorCopy(processor);
    EXPECT_EQ(processor._distributeFieldName, processorCopy._distributeFieldName);
    EXPECT_EQ(processor._rule, processorCopy._rule);
    EXPECT_EQ(processor._otherClusters, processorCopy._otherClusters);
    EXPECT_THAT(processor._otherClusters, UnorderedElementsAre("cluster4", "cluster5"));
    EXPECT_EQ(processor._emptyClusters, processorCopy._emptyClusters);
    EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre());
}

TEST_F(DistributeDocumentProcessorTest, testInitSuccess) {
    {
        // use default as other
        checkInit(_kvWithDefault, true);
        EXPECT_THAT(processor._otherClusters, UnorderedElementsAre("cluster4", "cluster5"));
        EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre());
    }
    {
        // no default, no other
        checkInit(_kvWithoutDefault, true);
        EXPECT_THAT(processor._otherClusters, UnorderedElementsAre());
        EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre());
    }
    {
        // use other
        KeyValueMap kvMap = _kvWithDefault;
        kvMap[DistributeDocumentProcessor::OTHER_CLUSTERS] = "cluster2,cluster5";
        checkInit(kvMap, true);
        EXPECT_THAT(processor._otherClusters, UnorderedElementsAre("cluster2", "cluster5"));
        EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre());
    }
    {
        // empty broadcast all
        KeyValueMap kvMap = _kvWithoutDefault;
        kvMap[DistributeDocumentProcessor::EMPTY_CLUSTERS] = "__all__";
        checkInit(kvMap, true);
        EXPECT_THAT(processor._otherClusters, UnorderedElementsAre());
        EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre("cluster1", "cluster2", "cluster3", "cluster4", "cluster5"));
    }
    {
        // empty clusters
        KeyValueMap kvMap = _kvWithoutDefault;
        kvMap[DistributeDocumentProcessor::EMPTY_CLUSTERS] = "cluster1,cluster2";
        checkInit(kvMap, true);
        EXPECT_THAT(processor._otherClusters, UnorderedElementsAre());
        EXPECT_THAT(processor._emptyClusters, UnorderedElementsAre("cluster1", "cluster2"));
    }
}

TEST_F(DistributeDocumentProcessorTest, testInitFail) {
    KeyValueMap kvEmpty;
    KeyValueMap kvWithoutRule;
    KeyValueMap kvInvalidRule;
    KeyValueMap kvEmptyDistribute;
    KeyValueMap kvInvalidCluster;

    kvEmptyDistribute[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "";

    kvWithoutRule[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "distribute";

    kvInvalidRule[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "distribute";
    kvInvalidRule[DistributeDocumentProcessor::DISTRIBUTE_RULE] = "1: cluster1,cluster2; 2";

    kvInvalidCluster[DistributeDocumentProcessor::DISTRIBUTE_FIELD_NAMES] = "distribute";
    kvInvalidCluster[DistributeDocumentProcessor::DISTRIBUTE_RULE] = "1: cluster1,cluster2; 2: cluster6";

    checkInitFail(kvEmpty);
    checkInitFail(kvEmptyDistribute);
    checkInitFail(kvInvalidRule);
    checkInitFail(kvWithoutRule);
}

TEST_F(DistributeDocumentProcessorTest, testUndefineDistributeField) {
    {
        // distributeField is invalid, but _defaultClusters is "cluster4" and "cluster5"
        // clusterMetas has "cluster1" ... "cluster5"
        // processedMetas will has "cluster4", "cluster5"
        ExtendDocumentPtr doc = createDocument("distribute", "-1",
                "cluster1,cluster2,cluster3,cluster4,cluster5");
        processDoc(_kvWithDefault, _clusterNames, doc);
        EXPECT_EQ("cluster4,cluster5", getClusterStr(doc));
    }
    {
        // distributeField is invalid, and  _defaultClusters empty
        // clusterMetas has "cluster1" ... "cluster5"
        // processedMetas will be empty
        ExtendDocumentPtr doc = createDocument("distribute", "-1",
                "cluster1,cluster2,cluster3,cluster4,cluster5");
        processDoc(_kvWithoutDefault, _clusterNames, doc);
        EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());
    }
    {
        // empty distribute rule
        ExtendDocumentPtr doc = createDocument("distribute", "",
                "cluster1,cluster2,cluster3,cluster4,cluster5");
        processDoc(_kvWithoutDefault, _clusterNames, doc);
        EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());
    }
    {
        // empty distribute rule with empty_clusters
        ExtendDocumentPtr doc = createDocument("distribute", "",
                "cluster1,cluster2,cluster3,cluster4,cluster5");
        KeyValueMap kvMap = _kvWithoutDefault;
        kvMap[DistributeDocumentProcessor::EMPTY_CLUSTERS] = "__all__";
        processDoc(kvMap, _clusterNames, doc);
        EXPECT_EQ(ADD_DOC, doc->getRawDocument()->getDocOperateType());
    }
}


TEST_F(DistributeDocumentProcessorTest, testSimpleProcess) {
    {
        // clusterMetas has "cluster1" ... "cluster5"
        // rule of "1" is "cluster1", "cluster2"
        // processedMetas will has "cluster1", "cluster2"
        ExtendDocumentPtr doc = createDocument("distribute", "1",
                "cluster1,cluster2,cluster3,cluster4,cluster5");
        processDoc(_kvWithDefault, _clusterNames, doc);
        EXPECT_EQ("cluster1,cluster2", getClusterStr(doc));
    }
    {
        // clusterMetas has "cluster1", "cluster3"
        // rule of "1" is "cluster1", "cluster2"
        // processedMetas will has "cluster1"
        ExtendDocumentPtr doc = createDocument("distribute", "1",
                "cluster1,cluster3");
        processDoc(_kvWithDefault, _clusterNames, doc);
        EXPECT_EQ("cluster1", getClusterStr(doc));
    }
    {
        // clusterMetas is empty
        // rule of "1" is "cluster1", "cluster2"
        // processedMetas will be empty
        ExtendDocumentPtr doc = createDocument("distribute", "1", "");
        processDoc(_kvWithDefault, _clusterNames, doc);
        EXPECT_EQ("", getClusterStr(doc));
        EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());
    }
}

TEST_F(DistributeDocumentProcessorTest, testProcessType) {
    DocumentProcessorPtr processor(new DistributeDocumentProcessor);
    DocumentProcessorPtr clonedProcessor(processor->clone());
    EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_SUB_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(UPDATE_FIELD));
    EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));
}

}
}
