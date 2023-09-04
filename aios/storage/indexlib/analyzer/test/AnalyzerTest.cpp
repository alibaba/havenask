#include "indexlib/analyzer/Analyzer.h"

#include <string>

#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/SimpleTokenizer.h"
#include "indexlib/analyzer/TraditionalTables.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using autil::codec::NormalizeOptions;

namespace indexlibv2 { namespace analyzer {

class AnalyzerTest : public TESTBASE
{
private:
    indexlib::document::AnalyzerToken _token;
};

#define ASSERT_TOKEN(tokenStr, position, normalizedText, stopWordFlag, isSpaceFlag, analyzerPtr)                       \
    {                                                                                                                  \
        bool ret = analyzerPtr->next(_token);                                                                          \
        ASSERT_TRUE(ret);                                                                                              \
        ASSERT_EQ(tokenStr, _token.getText());                                                                         \
        ASSERT_EQ((uint32_t)position, _token.getPosition());                                                           \
        ASSERT_EQ(normalizedText, _token.getNormalizedText());                                                         \
        ASSERT_EQ(stopWordFlag, _token.isStopWord());                                                                  \
        ASSERT_EQ(isSpaceFlag, _token.isSpace());                                                                      \
    }

TEST_F(AnalyzerTest, testSimpleTokenize)
{
    std::shared_ptr<AnalyzerInfo> info(new AnalyzerInfo);
    ITokenizer* tokenizer = new SimpleTokenizer("");
    Analyzer simpleAnalyzer(tokenizer);
    simpleAnalyzer.init(info.get(), nullptr);
    string text("20000:6428429027:6428129029:64290");
    simpleAnalyzer.tokenize(text.c_str(), text.length());
    ASSERT_TOKEN("20000:64284", 0, "20000:64284", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("", 1, "", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("29027:64281", 2, "29027:64281", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("", 3, "", false, false, (&simpleAnalyzer));
    ASSERT_TOKEN("29029:64290", 4, "29029:64290", false, false, (&simpleAnalyzer));
}
}} // namespace indexlibv2::analyzer
