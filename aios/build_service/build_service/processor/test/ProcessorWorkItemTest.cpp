#include "build_service/test/unittest.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/DocumentProcessorChainCreator.h"
#include "build_service/processor/test/MockDocumentProcessorChain.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;
using namespace build_service::document;
using namespace build_service::config;

IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {

class ProcessorWorkItemTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    ProcessorWorkItem *createWorkItem(const string &configName);
    RawDocumentVecPtr createRawDocVec(const string &rawDocString);
    void checkProcessedDocs(const ProcessedDocumentVecPtr& docs,
                            const string& pDocInfoStr);
    
protected:
    static DocumentProcessorChain *cloneMockChain(bool defaultReturn);
protected:
    DocProcessorChainConfigVecPtr _chainConfig;
    ProcessorMetricReporter *_reporter;
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};

void ProcessorWorkItemTest::setUp() {
    _reporter = new ProcessorMetricReporter();
    _counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
}

void ProcessorWorkItemTest::tearDown() {
    DELETE_AND_SET_NULL(_reporter);
}

ProcessorWorkItem *ProcessorWorkItemTest::createWorkItem(const string &configName)
{
    string configPath = TEST_DATA_PATH"/processor_work_item_test/" + configName;
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configPath));
    DocumentProcessorChainCreator creator;
    if (!creator.init(resourceReaderPtr, NULL, _counterMap)) {
        return NULL;
    }
    
    _chainConfig.reset(new DocProcessorChainConfigVec);
    if (!resourceReaderPtr->getDataTableConfigWithJsonPath(
                    "simple", "processor_chain_config", *_chainConfig))
    {
        return NULL;
    }
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    if (!resourceReaderPtr->getDataTableConfigWithJsonPath(
            "simple", "processor_chain_selector_config", *selectorConfig))
    {
        BS_LOG(INFO, "not processor_chain_selector_config");
    }
    ProcessorChainSelectorPtr selector(new ProcessorChainSelector);
    if (!selector->init(_chainConfig, selectorConfig))
    {
        return NULL;
    }

    DocumentProcessorChainVecPtr chains(new DocumentProcessorChainVec);
    for (size_t i = 0; i < _chainConfig->size(); ++i) {
        DocumentProcessorChainPtr chain =
            creator.create((*_chainConfig)[i], (*_chainConfig)[i].clusterNames);
        if (!chain) {
            return NULL;
        }
        chains->push_back(chain);
    }
    return new ProcessorWorkItem(chains, selector, _reporter);
}

RawDocumentVecPtr ProcessorWorkItemTest::createRawDocVec(const string &rawDocString)
{
    RawDocumentVecPtr rawDocs(new RawDocumentVec);
    vector<vector<string>> rawDocInfos;
    StringUtil::fromString(rawDocString, rawDocInfos, ":", ";");

    for (size_t i = 0; i < rawDocInfos.size(); i++) {
        assert(rawDocInfos[i].size() == 2);
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(i + 1);
        rawDoc->setField("field", string("f") + rawDocInfos[i][0]);
        rawDoc->setField("section", string("s") + rawDocInfos[i][0]);
        if (rawDocInfos[i][1] == "false") {
            rawDoc->setField("need_process_fail", "true");
        }
        rawDocs->push_back(rawDoc);
    }
    return rawDocs;
}

void ProcessorWorkItemTest::checkProcessedDocs(
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
            EXPECT_TRUE((*docs)[i]->getDocument());
            DocumentTestHelper::checkDocument(
                    FakeDocument(StringUtil::toString(pDocInfos[i][0]),
                            ADD_DOC, chainMap[pDocInfos[i][1]], true),
                    (*docs)[i]->getDocument());
        }
    }
}

TEST_F(ProcessorWorkItemTest, testSimpleProcess) {
    ProcessorWorkItemPtr workItem(createWorkItem("config/"));
    ASSERT_TRUE(workItem);
    uint32_t docSeed = 0;
    RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docSeed);
    workItem->initProcessData(rawDoc);
    workItem->process();
    ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
    ASSERT_TRUE(docs);
    DocumentTestHelper::checkDocument(0, docs);
}

DocumentProcessorChain *ProcessorWorkItemTest::cloneMockChain(bool defaultReturn) {
    StrictMockDocumentProcessorChain *mockChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*mockChain, processExtendDocument(_)).WillByDefault(Return(defaultReturn));
    EXPECT_CALL(*mockChain, processExtendDocument(_)).Times(1);
    return mockChain;
}

TEST_F(ProcessorWorkItemTest, testProcessChainFailed) {
    DocumentProcessorChainVecPtr chains(new DocumentProcessorChainVec);
    DocProcessorChainConfigVecPtr chainsConfigVec(new DocProcessorChainConfigVec);
    for (size_t i = 0; i < 3; ++i) {
        StrictMockDocumentProcessorChain *mockChain = new StrictMockDocumentProcessorChain;
        bool defaultReturn = (i != 1); // return false when i == 1
        ON_CALL(*mockChain, clone()).WillByDefault(
                Invoke(std::bind(&ProcessorWorkItemTest::cloneMockChain, defaultReturn)));
        EXPECT_CALL(*mockChain, clone());
        chains->push_back(DocumentProcessorChainPtr(mockChain));
        chainsConfigVec->push_back(DocProcessorChainConfig());
    }
    ProcessorChainSelectorPtr selector(new ProcessorChainSelector);
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    selector->init(chainsConfigVec, selectorConfig);
    ProcessorWorkItemPtr workItem(new ProcessorWorkItem(chains, selector, _reporter));
    RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(0);
    workItem->initProcessData(rawDoc);
    workItem->process();
    ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
    EXPECT_TRUE(docs);
    EXPECT_EQ(size_t(3), docs->size());
}

ACTION_P(Rewrite, op) {
    arg0->getRawDocument()->setDocOperateType(op);
    return true;
}

ACTION_P(CreateRewriteChain, op) {
    StrictMockDocumentProcessorChain *mockChain = new StrictMockDocumentProcessorChain;
    EXPECT_CALL(*mockChain, processExtendDocument(_)).WillOnce(Rewrite(op));
    return mockChain;
}

TEST_F(ProcessorWorkItemTest, testProcessSkipAndUnknown) {
    DocumentProcessorChainVecPtr chains(new DocumentProcessorChainVec);
    DocProcessorChainConfigVecPtr chainsConfigVec(new DocProcessorChainConfigVec);
    for (size_t i = 0; i < 4; ++i) {
        StrictMockDocumentProcessorChain *mockChain = new StrictMockDocumentProcessorChain;
        if (i == 1) {
            ON_CALL(*mockChain, clone()).WillByDefault(CreateRewriteChain(SKIP_DOC));
        } else if (i == 2) {
            ON_CALL(*mockChain, clone()).WillByDefault(CreateRewriteChain(UPDATE_FIELD));
        } else if (i == 3) {
            ON_CALL(*mockChain, clone()).WillByDefault(CreateRewriteChain(ADD_DOC));
        } else {
            ON_CALL(*mockChain, clone()).WillByDefault(
                    Invoke(std::bind(&ProcessorWorkItemTest::cloneMockChain, true)));
        }
        EXPECT_CALL(*mockChain, clone());
        chains->push_back(DocumentProcessorChainPtr(mockChain));
        chainsConfigVec->push_back(DocProcessorChainConfig());
    }

    ProcessorChainSelectorPtr selector(new ProcessorChainSelector);
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    selector->init(chainsConfigVec, selectorConfig);
    
    ProcessorWorkItemPtr workItem(new ProcessorWorkItem(chains, selector,  _reporter));
    RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(0, "unknown");
    workItem->initProcessData(rawDoc);
    workItem->process();
    ProcessedDocumentVecPtr vec = workItem->getProcessedDocs();
    EXPECT_TRUE(vec);
    EXPECT_EQ(size_t(4), vec->size());
}

TEST_F(ProcessorWorkItemTest, testProcessorMultiDoc) {
    ProcessorWorkItemPtr workItem(createWorkItem("config/"));
    ASSERT_TRUE(workItem);
    for (size_t i = 0; i < 10; ++i) {
        uint32_t docSeed = i;
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docSeed);
        workItem->initProcessData(rawDoc);
        workItem->process();
        ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
        ASSERT_TRUE(docs);
        DocumentTestHelper::checkDocument(docSeed, docs);
    }
}

TEST_F(ProcessorWorkItemTest, testBatchProcessMultiDocs) {
    ProcessorWorkItemPtr workItem(createWorkItem("config/"));
    ASSERT_TRUE(workItem);

    RawDocumentVecPtr rawDocs(new RawDocumentVec);
    for (size_t i = 0; i < 10; ++i) {
        uint32_t docSeed = i;
        RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(docSeed);
        rawDocs->push_back(rawDoc);
    }

    workItem->initBatchProcessData(rawDocs);
    workItem->process();
    ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
    ASSERT_TRUE(docs);

    ASSERT_EQ(20, docs->size());
    for (size_t i = 0; i < 10; ++i) {
        ProcessedDocumentVecPtr checkDocs(new ProcessedDocumentVec);
        checkDocs->push_back((*docs)[2 * i]);
        checkDocs->push_back((*docs)[2 * i + 1]);
        uint32_t docSeed = i;
        DocumentTestHelper::checkDocument(docSeed, checkDocs);
    }
}

TEST_F(ProcessorWorkItemTest, testBatchProcessDocWithSelector) {
    ProcessorWorkItemPtr workItem(createWorkItem("config_with_chain_selector/"));
    ASSERT_TRUE(workItem);

    // seed->1, chain1, valid
    // seed->2, chain3, valid
    // seed->3, chain2|chain1, valid
    // seed->4, chain2|chain1, invalid
    // seed->5, chain2|chain1, valid
    // seed->6, chain3, invalid
    // seed->7, chain1, valid
    // seed->8, chain3, valid
    // seed->9, chain1, invalid
    RawDocumentVecPtr rawDocs = createRawDocVec(
            "1:true;3:true;2:true;2:false;2:true;3:false;1:true;3:true;1:false");

    workItem->initBatchProcessData(rawDocs);
    workItem->process();
    ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
    ASSERT_TRUE(docs);
    checkProcessedDocs(docs, "1:1;2:3;3:2;3:1;4:-1;4:-1;5:2;5:1;6:-1;7:1;8:3;9:-1");
}

TEST_F(ProcessorWorkItemTest, testProcessDocWithSelector) {
    ProcessorWorkItemPtr workItem(createWorkItem("config_with_chain_selector/"));
    ASSERT_TRUE(workItem);
    
    RawDocumentPtr rawDoc = DocumentTestHelper::createRawDocStr(1);
    rawDoc->setField("field", "f1");
    rawDoc->setField("section", "s1");
    workItem->initProcessData(rawDoc);
    workItem->process();
    ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
    ASSERT_TRUE(docs);
    ASSERT_EQ(1, docs->size());

    {
        // test error
        ASSERT_EQ(0, workItem->_processErrorCount);
        rawDoc->setField("field", "no-exist");
        workItem->initProcessData(rawDoc);
        workItem->process();
        ProcessedDocumentVecPtr docs = workItem->getProcessedDocs();
        ASSERT_FALSE(docs);
        ASSERT_EQ(1, workItem->_processErrorCount);
    }
    {
        // mock selector
        workItem->_chainSelector.reset(new ProcessorChainSelector);
        workItem->_chainSelector->_allChainIds.push_back(0);
        workItem->_chainSelector->_allChainIds.push_back(1);
        workItem->_chainSelector->_allChainIds.push_back(2);
        workItem->initProcessData(rawDoc);
        workItem->process();
        docs = workItem->getProcessedDocs();
        ASSERT_TRUE(docs);
        ASSERT_EQ(3, docs->size());
    }
}

}
}
