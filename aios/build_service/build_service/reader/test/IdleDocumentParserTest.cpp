#include "build_service/test/unittest.h"
#include "build_service/reader/IdleDocumentParser.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
IE_NAMESPACE_USE(document);

namespace build_service {
namespace reader {

class IdleDocumentParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void IdleDocumentParserTest::setUp() {
}

void IdleDocumentParserTest::tearDown() {
}

TEST_F(IdleDocumentParserTest, testSimple) {
    IdleDocumentParser parser("data");

    DefaultRawDocument rawDoc;
    ASSERT_TRUE(parser.parse("abcdef", rawDoc));
    ASSERT_EQ(size_t(2), rawDoc.getFieldCount());
    EXPECT_EQ("abcdef", rawDoc.getField("data"));
    EXPECT_EQ(ADD_DOC, rawDoc.getDocOperateType());
}

}
}
