#include "build_service/test/unittest.h"
#include "build_service/analyzer/TextBuffer.h"

using namespace std;
using namespace autil;

namespace build_service {
namespace analyzer {

class TextBufferTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TextBufferTest::setUp() {
}

void TextBufferTest::tearDown() {
}

TEST_F(TextBufferTest, testNextOrignalTextFailed) {
    BS_LOG(DEBUG, "Begin testSingleWSTokenize!");

    TextBuffer textBuffer;
    textBuffer.resize(128);
    // invalid utf8 text
    string normalizeText("\x74\xCF\xDD\xEF\xBF\xBF\xFF\x20\xEF\xBF\x59");
    string text;
    bool ret = textBuffer.nextTokenOrignalText(normalizeText, text);
    ASSERT_TRUE(!ret);

    const char *str = "a阿　Ａ";
    normalizeText.assign(str);
    ret = textBuffer.nextTokenOrignalText(normalizeText, text);
    ASSERT_TRUE(!ret);
}


}
}
