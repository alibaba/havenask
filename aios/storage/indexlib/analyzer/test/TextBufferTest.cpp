#include "indexlib/analyzer/TextBuffer.h"

#include "autil/Log.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace analyzer {

class TextBufferTest : public TESTBASE
{
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.analyzer, TextBufferTest);

TEST_F(TextBufferTest, testNextOrignalTextFailed)
{
    TextBuffer textBuffer;
    textBuffer.resize(128);
    // invalid utf8 text
    string normalizeText("\x74\xCF\xDD\xEF\xBF\xBF\xFF\x20\xEF\xBF\x59");
    string text;
    bool ret = textBuffer.nextTokenOrignalText(normalizeText, text);
    ASSERT_TRUE(!ret);

    const char* str = "a阿　Ａ";
    normalizeText.assign(str);
    ret = textBuffer.nextTokenOrignalText(normalizeText, text);
    ASSERT_TRUE(!ret);
}

}} // namespace indexlibv2::analyzer
