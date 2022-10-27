#include "build_service/test/unittest.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/analyzer/Analyzer.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/TokenizerManager.h"

using namespace std;
using namespace autil;

using namespace build_service::config;
namespace build_service {
namespace analyzer {

class AnalyzerFactoryTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    AnalyzerFactory* _analyzerFactory;
    AnalyzerInfosPtr _infosPtr;
    config::ResourceReaderPtr _resourceReader;
};

void AnalyzerFactoryTest::setUp() {
    _resourceReader.reset(new ResourceReader(TEST_DATA_PATH"/analyzer_factory_test/"));

    _infosPtr.reset(new AnalyzerInfos());
    TraditionalTables tables;
    TraditionalTable table;
    table.tableName = "table_name";
    table.table[20094] = 20094;
    table.table[33879] = 33879;
    tables.addTraditionalTable(table);

    _infosPtr->setTraditionalTables(tables);

    AnalyzerInfo simpleInfo;
    simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    simpleInfo.setTokenizerConfig(DELIMITER, "\t");
    _infosPtr->addAnalyzerInfo("alimama", simpleInfo);

    AnalyzerInfo singleInfo;
    singleInfo.setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    _infosPtr->addAnalyzerInfo("singlews", singleInfo);

    _analyzerFactory = new AnalyzerFactory();
}

void AnalyzerFactoryTest::tearDown() {
    DELETE_AND_SET_NULL(_analyzerFactory);
}

TEST_F(AnalyzerFactoryTest, testInitSuccessful) {
    _infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(_resourceReader));
    ASSERT_TRUE(tokenizerManagerPtr->init(_infosPtr->getTokenizerConfig()));
    ASSERT_TRUE(_analyzerFactory->init(_infosPtr, tokenizerManagerPtr));
}

TEST_F(AnalyzerFactoryTest, testInitFailed) {
    AnalyzerInfo invalidInfo;
    invalidInfo.setTokenizerConfig(TOKENIZER_TYPE, "invalid");
    _infosPtr->addAnalyzerInfo("invalid", invalidInfo);

    _infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(_resourceReader));
    ASSERT_TRUE(tokenizerManagerPtr->init(_infosPtr->getTokenizerConfig()));
    ASSERT_FALSE(_analyzerFactory->init(_infosPtr, tokenizerManagerPtr));
}

TEST_F(AnalyzerFactoryTest, testCreateAnalyzer) {
    _infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(new TokenizerManager(_resourceReader));
    ASSERT_TRUE(tokenizerManagerPtr->init(_infosPtr->getTokenizerConfig()));
    ASSERT_TRUE(_analyzerFactory->init(_infosPtr, tokenizerManagerPtr));

    Analyzer* analyzer = _analyzerFactory->createAnalyzer("singlews");
    ASSERT_TRUE(analyzer);
    DELETE_AND_SET_NULL(analyzer);

    analyzer = _analyzerFactory->createAnalyzer("invalid");
    ASSERT_FALSE(analyzer);
}

#define ASSERT_TOKEN(tokenStr, position, normalizedText, stopWordFlag,  \
                     analyzerPtr) {                                     \
        bool ret = analyzerPtr->next(token);                            \
        ASSERT_TRUE(ret);                                               \
        ASSERT_EQ(string(tokenStr),token.getText());                    \
        ASSERT_EQ((uint32_t)position, token.getPosition());             \
        ASSERT_EQ(string(normalizedText),                               \
                  token.getNormalizedText());                           \
        ASSERT_EQ(stopWordFlag, token.isStopWord());                    \
    }

TEST_F(AnalyzerFactoryTest, testCreateFactorySuccess) {
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH"/analyzer_factory_test/"));
    AnalyzerFactoryPtr factory = AnalyzerFactory::create(resourceReader);
    ASSERT_TRUE(factory);

    Analyzer* analyzer = factory->createAnalyzer(string("singlews_analyzer"));
    ASSERT_TRUE(analyzer);

    string text = "Hello World. Are - you mm?";
    analyzer->tokenize(text.c_str(), text.length());
    Token token;
    ASSERT_TOKEN("Hello", 0, "hello", false, analyzer);
    ASSERT_TOKEN(" ", 1, " ", false, analyzer);
    ASSERT_TOKEN("World", 2, "world", false, analyzer);
    ASSERT_TOKEN(".", 3, ".", true, analyzer);
    ASSERT_TOKEN(" ", 4, " ", false, analyzer);
    ASSERT_TOKEN("Are", 5, "are", false, analyzer);
    ASSERT_TOKEN(" ", 6, " ", false, analyzer);
    ASSERT_TOKEN("-", 7, "-", true, analyzer);
    ASSERT_TOKEN(" ", 8, " ", false, analyzer);
    ASSERT_TOKEN("you", 9, "you", false, analyzer);
    ASSERT_TOKEN(" ", 10, " ", false, analyzer);
    ASSERT_TOKEN("mm", 11, "mm", true, analyzer);
    ASSERT_TOKEN("?", 12, "?", false, analyzer);
    ASSERT_TRUE(!analyzer->next(token));

    DELETE_AND_SET_NULL(analyzer);
}

TEST_F(AnalyzerFactoryTest, testNormalize) {
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH"/analyzer_core/"));
    AnalyzerFactoryPtr factory = AnalyzerFactory::create(resourceReader);
    ASSERT_TRUE(factory);

    Analyzer* analyzer = factory->createAnalyzer(string("singlews_analyzer"));
    ASSERT_TRUE(analyzer);

    string text = "Hello World. Are - you mm?阿里巴巴龍";
    analyzer->tokenize(text.c_str(), text.length());
    Token token;
    ASSERT_TOKEN("Hello", 0, "hello", false, analyzer);
    ASSERT_TOKEN(" ", 1, " ", false, analyzer);
    ASSERT_TOKEN("World", 2, "world", false, analyzer);
    ASSERT_TOKEN(".", 3, ".", true, analyzer);
    ASSERT_TOKEN(" ", 4, " ", false, analyzer);
    ASSERT_TOKEN("Are", 5, "bre", false, analyzer);
    ASSERT_TOKEN(" ", 6, " ", false, analyzer);
    ASSERT_TOKEN("-", 7, "-", true, analyzer);
    ASSERT_TOKEN(" ", 8, " ", false, analyzer);
    ASSERT_TOKEN("you", 9, "you", false, analyzer);
    ASSERT_TOKEN(" ", 10, " ", false, analyzer);
    ASSERT_TOKEN("mm", 11, "mm", true, analyzer);
    ASSERT_TOKEN("?", 12, "?", false, analyzer);
    ASSERT_TOKEN("阿", 13, "阿", false, analyzer);
    ASSERT_TOKEN("里", 14, "里", false, analyzer);
    ASSERT_TOKEN("巴", 15, "巴", false, analyzer);
    ASSERT_TOKEN("巴", 16, "巴", false, analyzer);
    ASSERT_TOKEN("龍", 17, "龙", false, analyzer);
    ASSERT_TRUE(!analyzer->next(token));

    DELETE_AND_SET_NULL(analyzer);
}

TEST_F(AnalyzerFactoryTest, testCreateFactoryFail) {
    ResourceReaderPtr resourceReader(new ResourceReader("not_exist_path"));
    AnalyzerFactoryPtr factory = AnalyzerFactory::create(resourceReader);
    ASSERT_TRUE(!factory);
}


}
}
