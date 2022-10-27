#include "build_service/test/unittest.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include <swift/common/FieldGroupWriter.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
SWIFT_USE_NAMESPACE(common);
IE_NAMESPACE_USE(document);
using namespace build_service::document;

namespace build_service {
namespace reader {

class SwiftFieldFilterRawDocumentParserTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    RawDocumentHashMapManagerPtr _hashMapManager;
};

void SwiftFieldFilterRawDocumentParserTest::setUp() {
    _hashMapManager.reset(new RawDocumentHashMapManager());
}

void SwiftFieldFilterRawDocumentParserTest::tearDown() {
}

TEST_F(SwiftFieldFilterRawDocumentParserTest, testSimple) {
    SwiftFieldFilterRawDocumentParser parser;
    {
        FieldGroupWriter writer;
        writer.addProductionField("name1", "value1");
        writer.addProductionField("name2", "value2");
        writer.addProductionField("name3", "value3", true);
        writer.addProductionField("name4", "");

        DefaultRawDocument rawDocument(_hashMapManager);
        EXPECT_FALSE(parser.parse("", rawDocument));
        ASSERT_TRUE(parser.parse(writer.toString(), rawDocument));
        ASSERT_EQ(size_t(5), rawDocument.getFieldCount());
        EXPECT_EQ("value1", rawDocument.getField("name1"));
        EXPECT_EQ("value2", rawDocument.getField("name2"));
        EXPECT_EQ("value3", rawDocument.getField("name3"));
        EXPECT_EQ("", rawDocument.getField("name4"));
        EXPECT_EQ("add", rawDocument.getField("CMD"));
        EXPECT_TRUE(rawDocument.exist("name4"));
    }
    {
        FieldGroupWriter writer;
        DefaultRawDocument rawDocument(_hashMapManager);
        writer.addProductionField("CMD", "delete");
        writer.addProductionField("name1", "value1");

        // empty field
        writer.addProductionField("name2", "");
        ASSERT_TRUE(parser.parse(writer.toString(), rawDocument));
        ASSERT_EQ(size_t(3), rawDocument.getFieldCount());
        EXPECT_EQ("value1", rawDocument.getField("name1"));
        EXPECT_EQ("delete", rawDocument.getField("CMD"));
        EXPECT_EQ("", rawDocument.getField("name2"));
        EXPECT_TRUE(rawDocument.exist("name2"));
    }
}

}
}
