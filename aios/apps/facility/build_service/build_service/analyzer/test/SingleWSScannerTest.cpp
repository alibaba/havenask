#include "build_service/analyzer/SingleWSScanner.h"

#include "autil/StringUtil.h"
#include "build_service/test/unittest.h"

using namespace std;

namespace build_service { namespace analyzer {

class SingleWSScannerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    void checkWSResult(const std::string& oriText, const std::string& checkText,
                       const std::vector<int32_t>& checkVec = std::vector<int32_t>());
};

void SingleWSScannerTest::setUp() {}

void SingleWSScannerTest::tearDown() {}

TEST_F(SingleWSScannerTest, testSimpleProcess)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "_abc cdf";
    string checkText = "_\1abc\1 \1cdf";
    checkWSResult(text, checkText);
}

void SingleWSScannerTest::checkWSResult(const string& oriText, const string& checkText, const vector<int32_t>& checkVec)
{
    istringstream iss(oriText);
    SingleWSScanner scanner(&iss, NULL);
    const char* pToken = NULL;
    int len = 0;

    vector<string> tokenVec = autil::StringUtil::split(checkText, "\1", false);
    for (size_t i = 0; i < tokenVec.size(); ++i) {
        ASSERT_TRUE(scanner.lex(pToken, len));
        BS_LOG(DEBUG, "(tokenVec[i]=%s), (ptoken=%s), len=%d", tokenVec[i].c_str(), pToken, len);
        ASSERT_EQ(tokenVec[i], string(pToken, len));
    }

    ASSERT_TRUE(!scanner.lex(pToken, len));
    ASSERT_TRUE(!pToken);
    ASSERT_EQ(0, len);

    ASSERT_EQ(!checkVec.empty(), scanner.hasInvalidChar());
    ASSERT_TRUE(checkVec == scanner.getInvalidCharPosVec());
}

TEST_F(SingleWSScannerTest, testChineseCharactor)
{
    BS_LOG(DEBUG, "Begin testChineseCharactor!");
    string text = "测试中文";
    string checkText = "测\1试\1中\1文";
    checkWSResult(text, checkText);

    text = " 包含符号，的中文word）（  ，半角\u3000a";
    checkText = " \1包\1含\1符\1号\1，\1的\1中\1文\1word\1）\1（\1 \1 \1，\1半\1角\1\u3000\1a";
    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testAlphaAndNum)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "this's an English sentence. Sentence1, 2, [word]. anti-spam";
    string checkText = "this's\1 \1an\1 \1English\1 \1sentence\1."
                       "\1 \1Sentence\0011\1,\1 \0012\1,\1 \1[\1word\1]\1.\1 \1anti\1-\1spam";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testFullWidthAlphaAndNum)
{
    BS_LOG(DEBUG, "Begin Test FullWidthAlphaAndNum!");
    string text = "ａｚＡＺazAZ　０９１091";
    string checkText = "ａｚＡＺazAZ\1　\1０９１091";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testUnsupportedUTF8Character)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "\366unsupported\366 UTF8";
    string checkText = "\1unsupported\1\1 \1UTF\0018";
    vector<int32_t> checkVec;
    autil::StringUtil::fromString("0 12", checkVec, " ");
    checkWSResult(text, checkText, checkVec);
}

TEST_F(SingleWSScannerTest, testSymbolChar)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "*?*D&G *H&M*K+D?";
    string checkText = "*\1?\1*\1D&G\1 \1*\1H&M\1*\1K+D\1?";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testSequencialSymbolChar)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "++++&&&&&'''";
    string checkText = "++++&&&&&'''";
    checkWSResult(text, checkText);

    text = "aa++++&&&&&'''";
    checkText = "aa++++&&&&&'''";
    checkWSResult(text, checkText);

    text = "++++&&&&&bb";
    checkText = "++++&&&&&bb";
    checkWSResult(text, checkText);

    text = "++++&cc&&&&";
    checkText = "++++&cc&&&&";
    checkWSResult(text, checkText);

    text = "a++++&& &&&";
    checkText = "a++++&&\1 \1&&&";
    checkWSResult(text, checkText);

    text = "a++++&&2&&&";
    checkText = "a++++&&\0012\1&&&";
    checkWSResult(text, checkText);

    text = "&\\+";
    checkText = "&\1\\\1+";
    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testFullWidthSymbolChar)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "×？×　×Ｄ&Ｇ　＊　？　！ｈ＆M？a‘b’c？";

    string checkText = "×\1？\1×\1　\1×\1Ｄ&Ｇ\1　\1＊\1　\1？\1　\1！\1ｈ＆M\1？\1a‘b’c\1？";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testSymbolAtFront)
{
    BS_LOG(DEBUG, "Begin Test symbol at front!");
    string text = "&g";
    string checkText = "&g";
    checkWSResult(text, checkText);

    text = "&";
    checkText = "&";
    checkWSResult(text, checkText);

    text = "+";
    checkText = "+";
    checkWSResult(text, checkText);

    text = "+a";
    checkText = "+a";
    checkWSResult(text, checkText);

    text = "+23";
    checkText = "+\00123";
    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testPlusChar)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "c++ ++c c+c c＋c ｃ＋＋　＋＋ｃ　ｃ＋ｃ　ｃ+ｃ c＋Ｃ";
    string checkText = "c++\1 \1++c\1 \1c+c\1 \1c＋c\1 \1ｃ＋＋\1　\1"
                       "＋＋ｃ\1　\1ｃ＋ｃ\1　\1ｃ+ｃ\1 \1c＋Ｃ";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testSymbolAlphaNUM)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "?a123*?+b*淘32f5*tp-link路由器";
    string checkText = "?\1a\001123\1*\1?\1+b\1*\1淘\00132\1f\0015\1*"
                       "\1tp\1-\1link\1路\1由\1器";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testFloat)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "★皇冠★直销H-M3M（1.2x1.8米）1/2 2/ /2 .2 2.";
    string checkText = "★\1皇\1冠\1★\1直\1销\1H\1-\1M\0013\1M\1"
                       "（\0011.2\1x\0011.8\1米\1）\0011/2\1 \001"
                       "2\1/\1 \1/\0012\1 \1.\0012\1 \0012\1.";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testHtmlEscape)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "d&amp;g &nbsp; a;b&lt;1中&gt;文";
    string checkText = "d&amp;g\1 \1&nbsp;\1 \1a\1;\1b&lt;\0011\1"
                       "中\1&gt;\1文";
    checkWSResult(text, checkText);

    text = "2d&amp;3";
    checkText = "2\1d&amp;\0013";
    checkWSResult(text, checkText);

    text = "2d&ampccc;3";
    checkText = "2\1d&ampccc;\0013";
    checkWSResult(text, checkText);

    text = "2d&ab;3";
    checkText = "2\1d&ab;\0013";
    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testWrongHtmlEscape)
{
    BS_LOG(DEBUG, "Begin Test!");
    string text = "d&aaabbmp;g &n; a;b&lt;1中&gt;文";
    string checkText = "d&aaabbmp\1;\1g\1 \1&n\1;\1 \1a\1;\1b&lt;\0011\1"
                       "中\1&gt;\1文";
    checkWSResult(text, checkText);

    text = "d&aaamp;g &a;g";
    checkText = "d&aaamp;g\1 \1&a\1;\1g";
    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testAllFloat)
{
    BS_LOG(DEBUG, "Begin Test all float!");
    string text = "1.2/3.4.5/6/7";
    string checkText = "1.2\001/\0013.4\001.\0015/6\001/\0017";

    checkWSResult(text, checkText);
}

TEST_F(SingleWSScannerTest, testStopWord)
{
    BS_LOG(DEBUG, "Begin testStopWord!");
    {
        string sym[] = {"&", "＆", "+", "＋", "'", "＇", "‘", "’", "&&", "&&''++"};
        vector<string> vec(sym, sym + 10);
        BS_LOG(DEBUG, "vec size is : %d", (int)vec.size());
        for (uint32_t i = 0; i < vec.size(); ++i) {
            istringstream iss(vec[i]);
            SingleWSScanner scanner(&iss, NULL);
            const char* pToken = NULL;
            int len = 0;

            ASSERT_TRUE(scanner.lex(pToken, len));
            ASSERT_EQ(vec[i], string(pToken, len));
            ASSERT_TRUE(scanner.isStopWord());
        }
    }
    {
        istringstream iss("8++9");
        SingleWSScanner scanner(&iss, NULL);
        const char* pToken = NULL;
        int len = 0;

        ASSERT_TRUE(scanner.lex(pToken, len));
        ASSERT_EQ(string("8"), string(pToken, len));
        ASSERT_TRUE(!scanner.isStopWord());

        ASSERT_TRUE(scanner.lex(pToken, len));
        ASSERT_EQ(string("++"), string(pToken, len));
        ASSERT_TRUE(scanner.isStopWord());

        ASSERT_TRUE(scanner.lex(pToken, len));
        ASSERT_EQ(string("9"), string(pToken, len));
        ASSERT_TRUE(!scanner.isStopWord());

        ASSERT_TRUE(!scanner.lex(pToken, len));
        ASSERT_TRUE(!pToken);
        ASSERT_EQ(0, len);
    }
}

}} // namespace build_service::analyzer
