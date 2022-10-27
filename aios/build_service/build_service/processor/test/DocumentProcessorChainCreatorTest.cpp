#include "build_service/test/unittest.h"
#include "build_service/processor/DocumentProcessorChainCreator.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/MainSubDocProcessorChain.h"
#include "build_service/processor/HashDocumentProcessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ConfigDefine.h"
#include <indexlib/util/counter/counter_map.h>

using namespace std;
using namespace build_service::config;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace processor {

class DocumentProcessorChainCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void checkProcessors(const DocumentProcessorChainCreator &creator,
                         const DocProcessorChainConfig &config,
                         const vector<string> &clusterNames,
                         bool hasSub = false, bool hasAdd2UpdateRewriter = false);

    void checkProcessorInfos(const ProcessorInfos& processorInfos,
                             SingleDocProcessorChain *singleChain,
                             bool isSub,
                             const vector<string> &clusterNames);

private:
    IE_NAMESPACE(util)::CounterMapPtr _counterMap;
};


void DocumentProcessorChainCreatorTest::setUp() {
    _counterMap.reset(new IE_NAMESPACE(util)::CounterMap());
}

void DocumentProcessorChainCreatorTest::tearDown() {
}

TEST_F(DocumentProcessorChainCreatorTest, testInit) {
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_success/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));
}

TEST_F(DocumentProcessorChainCreatorTest, testInitFailed) {
    // the only way to init fail : init analyzer fail
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/init_failed/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_FALSE(creator.init(resourceReaderPtr, NULL, _counterMap));
}

TEST_F(DocumentProcessorChainCreatorTest, testDiffTableName) {
    // the only way to init fail : init analyzer fail
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/diff_table_name/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));
    DocProcessorChainConfigVec docProcessorChainConfigVec;
    ASSERT_TRUE(resourceReaderPtr->getDataTableConfigWithJsonPath(
                    "simple", "processor_chain_config", docProcessorChainConfigVec));
    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[0], docProcessorChainConfigVec[0].clusterNames));
}

TEST_F(DocumentProcessorChainCreatorTest, testCreateFailed) {
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_failed/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));

    DocProcessorChainConfigVec docProcessorChainConfigVec;
    ASSERT_TRUE(resourceReaderPtr->getDataTableConfigWithJsonPath(
                    "simple", "processor_chain_config", docProcessorChainConfigVec));

    // table not exist
    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[0], docProcessorChainConfigVec[0].clusterNames));

    // main chain plugin so not exist
    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[1], docProcessorChainConfigVec[1].clusterNames));

    // main chain processor not exist
    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[2], docProcessorChainConfigVec[2].clusterNames));

    // sub chain create failed
    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[3], docProcessorChainConfigVec[3].clusterNames));
}

TEST_F(DocumentProcessorChainCreatorTest, testInitParamForAdd2UpdateRewriter) {
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_success/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));

    vector<string> clusterNames;
    clusterNames.push_back("simple_cluster");
    clusterNames.push_back("simple_cluster2");
    clusterNames.push_back("simple_cluster3");

    IndexPartitionSchemaPtr schema =
        resourceReaderPtr->getSchemaBySchemaTableName("simple_table");

    vector<IndexPartitionOptions> optionsVec;
    vector<SortDescriptions> sortDescVec;
    creator.initParamForAdd2UpdateRewriter(schema, clusterNames, 
            optionsVec, sortDescVec);

    ASSERT_EQ((size_t)3, optionsVec.size());
    ASSERT_EQ((size_t)3, sortDescVec.size());

    ASSERT_EQ((uint32_t)10, optionsVec[0].GetBuildConfig(false).maxDocCount);
    ASSERT_TRUE(optionsVec[0].GetMergeConfig().truncateOptionConfig);
    ASSERT_EQ((size_t)1, 
              optionsVec[0].GetMergeConfig().truncateOptionConfig->GetTruncateIndexConfigs().size());

    ASSERT_EQ((uint32_t)20, optionsVec[1].GetBuildConfig(false).maxDocCount);
    ASSERT_TRUE(!optionsVec[1].GetMergeConfig().truncateOptionConfig);
    ASSERT_EQ((uint32_t)30, optionsVec[2].GetBuildConfig(false).maxDocCount);
    ASSERT_TRUE(!optionsVec[2].GetMergeConfig().truncateOptionConfig);

    ASSERT_EQ((size_t)1, sortDescVec[0].size());
    ASSERT_TRUE(sortDescVec[0][0].sortFieldName == "id");

    ASSERT_EQ((size_t)1, sortDescVec[1].size());
    ASSERT_TRUE(sortDescVec[1][0].sortFieldName == "price2");

    ASSERT_EQ((size_t)0, sortDescVec[2].size());
}

TEST_F(DocumentProcessorChainCreatorTest, testCreateAdd2UpdateRewriterFail) {
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_rewrite_failed/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));

    vector<string> clusterNames;
    clusterNames.push_back("simple_cluster");

    DocProcessorChainConfigVec docProcessorChainConfigVec;
    ASSERT_TRUE(resourceReaderPtr->getDataTableConfigWithJsonPath(
                    "simple", "processor_chain_config", docProcessorChainConfigVec));

    ASSERT_FALSE(creator.create(docProcessorChainConfigVec[0], clusterNames));
}

TEST_F(DocumentProcessorChainCreatorTest, testCreateSuccess) {
    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_success/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));

    DocumentProcessorChainCreator creator;
    ASSERT_TRUE(creator.init(resourceReaderPtr, NULL, _counterMap));

    DocProcessorChainConfigVec docProcessorChainConfigVec;
    ASSERT_TRUE(resourceReaderPtr->getDataTableConfigWithJsonPath(
                    "simple", "processor_chain_config", docProcessorChainConfigVec));

    // default chain
    vector<string> clusterNames;
    clusterNames.push_back("simple_cluster");
    checkProcessors(creator, docProcessorChainConfigVec[0], clusterNames);

    // user defined plugins
    clusterNames.clear();
    clusterNames.push_back("simple_cluster2");
    clusterNames.push_back("simple_cluster3");
    checkProcessors(creator, docProcessorChainConfigVec[1], clusterNames);

    // sub chain
    clusterNames.clear();
    clusterNames.push_back("simple_with_sub_cluster2");
    checkProcessors(creator, docProcessorChainConfigVec[2], clusterNames, true);

    // sub chain with no processor
    clusterNames.clear();
    clusterNames.push_back("simple_with_sub_cluster3");
    checkProcessors(creator, docProcessorChainConfigVec[3], clusterNames, true);

    // has add2update rewriter
    clusterNames.clear();
    clusterNames.push_back("simple_cluster");
    checkProcessors(creator, docProcessorChainConfigVec[4], clusterNames, false, true);
}

#define ADD_PROCESSOR_INFO(processorInfoInConfig, c, m) \
    {                                                   \
        ProcessorInfo processorInfo;                    \
        processorInfo.className = c;                    \
        processorInfo.moduleName = m;                   \
        processorInfoInConfig.push_back(processorInfo); \
    }

TEST_F(DocumentProcessorChainCreatorTest, testNeedAdd2UpdateRewriter) {
    ProcessorInfos processorInfoInConfig;
    ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor", "user_module");
    ADD_PROCESSOR_INFO(processorInfoInConfig, "TokenizeDocumentProcessor", "");

    // test no need add2update rewriter
    ASSERT_FALSE(DocumentProcessorChainCreator::needAdd2UpdateRewriter(
                    processorInfoInConfig));

    // test need add2update rewriter
    ADD_PROCESSOR_INFO(processorInfoInConfig, MODIFIED_FIELDS_DOCUMENT_PROCESSOR_NAME, "");
    ASSERT_TRUE(DocumentProcessorChainCreator::needAdd2UpdateRewriter(
                    processorInfoInConfig));
}

TEST_F(DocumentProcessorChainCreatorTest, testConstructProcessorInfos) {
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "TokenizeDocumentProcessor", "");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, false, false);

        ASSERT_EQ(size_t(3), infos.size());
        EXPECT_EQ("HashDocumentProcessor", infos[0].className);
        EXPECT_EQ("UserDocumentProcessor", infos[1].className);
        EXPECT_EQ("TokenizeDocumentProcessor", infos[2].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig,"UserDocumentProcessor", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig,"TokenizeDocumentProcessor", "");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, true, false);

        ASSERT_EQ(size_t(4), infos.size());
        EXPECT_EQ("HashDocumentProcessor", infos[0].className);
        EXPECT_EQ("SubDocumentExtractorProcessor", infos[1].className);
        EXPECT_EQ("UserDocumentProcessor", infos[2].className);
        EXPECT_EQ("TokenizeDocumentProcessor", infos[3].className);
    }
    {
        ProcessorInfos processorInfoInConfig;

        ADD_PROCESSOR_INFO(processorInfoInConfig,"UserDocumentProcessor", "user_module");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, false, false);

        ASSERT_EQ(size_t(2), infos.size());
        EXPECT_EQ("HashDocumentProcessor", infos[0].className);
        EXPECT_EQ("UserDocumentProcessor", infos[1].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, false, true);

        ASSERT_EQ(size_t(2), infos.size());
        EXPECT_EQ("HashDocumentProcessor", infos[0].className);
        EXPECT_EQ("LineDataDocumentProcessor", infos[1].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "TokenizeDocumentProcessor", "");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, true, false, false);

        ASSERT_EQ(size_t(4), infos.size());
        EXPECT_EQ("RegionDocumentProcessor", infos[0].className);     
        EXPECT_EQ("HashDocumentProcessor", infos[1].className);
        EXPECT_EQ("UserDocumentProcessor", infos[2].className);
        EXPECT_EQ("TokenizeDocumentProcessor", infos[3].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor1", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor2", "user_module"); 
        ADD_PROCESSOR_INFO(processorInfoInConfig, "HashDocumentProcessor", "");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, true, false, false); 

        ASSERT_EQ(size_t(4), infos.size());
        EXPECT_EQ("RegionDocumentProcessor", infos[0].className);     
        EXPECT_EQ("UserDocumentProcessor1", infos[1].className);     
        EXPECT_EQ("UserDocumentProcessor2", infos[2].className);
        EXPECT_EQ("HashDocumentProcessor", infos[3].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor1", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor2", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "RegionDocumentProcessor", "");        
        ADD_PROCESSOR_INFO(processorInfoInConfig, "HashDocumentProcessor", "");

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, false, false); 

        ASSERT_EQ(size_t(4), infos.size());
        EXPECT_EQ("UserDocumentProcessor1", infos[0].className);     
        EXPECT_EQ("UserDocumentProcessor2", infos[1].className);
        EXPECT_EQ("RegionDocumentProcessor", infos[2].className);        
        EXPECT_EQ("HashDocumentProcessor", infos[3].className);
    }
    {
        ProcessorInfos processorInfoInConfig;
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor1", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "UserDocumentProcessor2", "user_module");
        ADD_PROCESSOR_INFO(processorInfoInConfig, "RegionDocumentProcessor", "");        

        ProcessorInfos infos = DocumentProcessorChainCreator::constructProcessorInfos(
                processorInfoInConfig, false, false, false); 

        ASSERT_EQ(size_t(4), infos.size());
        EXPECT_EQ("UserDocumentProcessor1", infos[0].className);     
        EXPECT_EQ("UserDocumentProcessor2", infos[1].className);
        EXPECT_EQ("RegionDocumentProcessor", infos[2].className);        
        EXPECT_EQ("HashDocumentProcessor", infos[3].className);
    }        
}

void DocumentProcessorChainCreatorTest::checkProcessors(
        const DocumentProcessorChainCreator &creator,
        const DocProcessorChainConfig& config,
        const vector<string> &clusterNames,
        bool hasSub, bool hasAdd2UpdateRewriter)
{
    DocumentProcessorChainPtr chain = creator.create(config, clusterNames);
    ASSERT_TRUE(chain->_docFactoryWrapper.get());
    ASSERT_TRUE(chain->_docParser.get());

    IE_NAMESPACE(document)::BuiltInParserInitParamPtr builtInParam =
        DYNAMIC_POINTER_CAST(IE_NAMESPACE(document)::BuiltInParserInitParam,
                             chain->_docParser->GetInitParam());
    ASSERT_TRUE(builtInParam);
    
    if (!hasSub) {
        SingleDocProcessorChain *singleChain = ASSERT_CAST_AND_RETURN(
                SingleDocProcessorChain, chain.get());
        checkProcessorInfos(config.processorInfos, singleChain, false, clusterNames);
        ASSERT_EQ(!hasAdd2UpdateRewriter, builtInParam->GetResource().docRewriters.empty());
    } else {
        MainSubDocProcessorChain *mainSubChain = ASSERT_CAST_AND_RETURN(
                MainSubDocProcessorChain, chain.get());
        ASSERT_EQ(!hasAdd2UpdateRewriter, builtInParam->GetResource().docRewriters.empty());

        SingleDocProcessorChain *mainChain = ASSERT_CAST_AND_RETURN(
                SingleDocProcessorChain, mainSubChain->_mainDocProcessor);
        SingleDocProcessorChain *subChain = ASSERT_CAST_AND_RETURN(
                SingleDocProcessorChain, mainSubChain->_subDocProcessor);

        ASSERT_FALSE(mainChain->_docParser.get());
        ASSERT_FALSE(subChain->_docParser.get());
        checkProcessorInfos(config.processorInfos, mainChain, true, clusterNames);
        checkProcessorInfos(config.subProcessorInfos, subChain, false, clusterNames);
    }
}

void DocumentProcessorChainCreatorTest::checkProcessorInfos(
        const ProcessorInfos& processorInfos,
        SingleDocProcessorChain *singleChain, bool hasSubParser,
        const vector<string> &clusterNames)
{
    ASSERT_TRUE(singleChain);
    const std::vector<DocumentProcessor*>& vec = singleChain->getDocumentProcessors();
    std::vector<DocumentProcessor*>::const_iterator it = vec.begin();

    HashDocumentProcessor *processor = ASSERT_CAST_AND_RETURN(
            HashDocumentProcessor, (*it));
    EXPECT_EQ(string("HashDocumentProcessor"), (*it++)->getDocumentProcessorName());
    ASSERT_TRUE(processor);
    vector<string> actualClusters;
    for (size_t i = 0; i < processor->_clusterNames.size(); i++) {
        actualClusters.push_back(processor->_clusterNames[i]);
    }
    EXPECT_THAT(actualClusters, testing::UnorderedElementsAreArray(clusterNames));
    if (hasSubParser) {
        EXPECT_EQ(string("SubDocumentExtractorProcessor"), (*it)->getDocumentProcessorName());
        ++it;
    }

    // plus 2 build in processor
    ASSERT_EQ(processorInfos.size(), size_t(vec.end() - it));
    for (size_t i = 0; i < processorInfos.size(); ++i, ++it) {
        EXPECT_EQ(processorInfos[i].className, (*it)->getDocumentProcessorName());
    }
    EXPECT_EQ(vec.end(), it);
}

}
}
