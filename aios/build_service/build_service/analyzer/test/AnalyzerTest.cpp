#include "build_service/test/unittest.h"
#include "build_service/analyzer/Analyzer.h"
#include "build_service/analyzer/SingleWSTokenizer.h"
#include "build_service/analyzer/SimpleTokenizer.h"
#include "build_service/analyzer/AnalyzerDefine.h"
#include "build_service/analyzer/AnalyzerInfo.h"
#include "build_service/analyzer/TraditionalTables.h"
#include <string>

using namespace std;
using namespace autil;

using namespace build_service::util;
using namespace build_service::config;
namespace build_service {
namespace analyzer {

class AnalyzerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    Token _token;
};

#define ASSERT_TOKEN(tokenStr, position, normalizedText, stopWordFlag,  \
                     isSpaceFlag, analyzerPtr) {                        \
        bool ret = analyzerPtr->next(_token);                           \
        ASSERT_TRUE(ret);                                               \
        ASSERT_EQ(tokenStr,_token.getText());                           \
        ASSERT_EQ((uint32_t)position, _token.getPosition());            \
        ASSERT_EQ(normalizedText,                                       \
                  _token.getNormalizedText());                          \
        ASSERT_EQ(stopWordFlag, _token.isStopWord());                   \
        ASSERT_EQ(isSpaceFlag, _token.isSpace());                       \
    }

void AnalyzerTest::setUp() {
}

void AnalyzerTest::tearDown() {
}

TEST_F(AnalyzerTest, testSingleWSTokenize) {
    BS_LOG(DEBUG, "Begin testSingleWSTokenize!");

    AnalyzerInfoPtr info(new AnalyzerInfo);
    info->setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    info->setTokenizerConfig(TOKENIZER_ID, "INTERNET_CHN");
    info->addStopWord(string(","));
    info->addStopWord(string("+"));
    info->addStopWord(string(";"));
    info->addStopWord(string("　"));
    info->addStopWord(string("("));
    info->addStopWord(string(")"));
    NormalizeOptions option;
    option.caseSensitive = false;
    option.traditionalSensitive = false;
    option.widthSensitive = false;
    info->setNormalizeOptions(option);

    Tokenizer* tokenizer = new SingleWSTokenizer();
    Analyzer singleAnalyzer(tokenizer);
    singleAnalyzer.init(info.get(), NULL);
    string text("Hello,你好!&&++8d&g99levi's1.2/3a&amp;b");
    singleAnalyzer.tokenize(text.c_str(), text.length());
    ASSERT_TOKEN("Hello", 0, "hello", false, false, (&singleAnalyzer));
    ASSERT_TOKEN(",", 1, ",", true, false, (&singleAnalyzer));
    ASSERT_TOKEN("你", 2, "你", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("好", 3, "好", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("!", 4, "!", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("&&++", 5, "&&++", true, false, (&singleAnalyzer));
    ASSERT_TOKEN("8", 6, "8", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("d&g", 7, "d&g", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("99", 8, "99", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("levi's", 9, "levi's", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("1.2", 10, "1.2", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("/", 11, "/", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("3", 12, "3", false, false, (&singleAnalyzer));
    ASSERT_TOKEN("a&amp;b", 13, "a&amp;b", false, false, (&singleAnalyzer));
}

TEST_F(AnalyzerTest, testSimpleTokenize) {
    BS_LOG(DEBUG, "Begin test!");

    AnalyzerInfoPtr info(new AnalyzerInfo);
    Tokenizer* tokenizer = new SimpleTokenizer("");
    Analyzer simpleAnalyzer(tokenizer);
    simpleAnalyzer.init(info.get(), NULL);
    string text("20000:6428429027:6428129029:64290");
    simpleAnalyzer.tokenize(text.c_str(), text.length());
    ASSERT_TOKEN("20000:64284", 0, "20000:64284", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("", 1, "", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("29027:64281", 2, "29027:64281", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("", 3, "", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("29029:64290", 4, "29029:64290", false, false, (&simpleAnalyzer));
}


}
}
