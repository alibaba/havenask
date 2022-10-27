#include "build_service/test/unittest.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/document/RawDocument.h"
#include "build_service/workflow/RawDocBuilderConsumer.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace build_service::builder;
using namespace build_service::document;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace workflow {

class RawDocBuilderConsumerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    static bool mockBuild(const DocumentPtr &doc, vector<DocumentPtr> *docVec) {
        docVec->push_back(doc);
        return true;
    }
    static RawDocumentPtr createDoc(const common::Locator &locator)
    {
        // doc 1
        RawDocumentPtr rawDoc(new DefaultRawDocument());
        rawDoc->setLocator(locator);
        return rawDoc;
    }
};

void RawDocBuilderConsumerTest::setUp() {
}

void RawDocBuilderConsumerTest::tearDown() {
}

TEST_F(RawDocBuilderConsumerTest, testConsume) {
    vector<DocumentPtr> buildedDocs;
    StrictMockBuilder builder;
    RawDocBuilderConsumer consumer(&builder);
    consumer.SetEndBuildTimestamp(101);
    ON_CALL(builder, doBuild(_)).WillByDefault(
            Invoke(std::bind(&mockBuild, std::placeholders::_1, &buildedDocs)));
    EXPECT_CALL(builder, doBuild(_)).Times(3);
    EXPECT_CALL(builder, stop(101)).Times(1);

    RawDocumentPtr rawDoc;

    common::Locator locator1(1);
    rawDoc = createDoc(locator1);
    ASSERT_EQ(FE_OK, consumer.consume(rawDoc));

    common::Locator locator2(2);
    rawDoc = createDoc(locator2);
    ASSERT_EQ(FE_OK, consumer.consume(rawDoc));

    common::Locator locator3(3);
    rawDoc = createDoc(locator3);
    ASSERT_EQ(FE_OK, consumer.consume(rawDoc));

    ASSERT_EQ(size_t(3), buildedDocs.size());

    consumer.stop(FE_EOF);

    ASSERT_EQ(Locator(locator1.toString()), buildedDocs[0]->GetLocator());
    ASSERT_EQ(Locator(locator2.toString()), buildedDocs[1]->GetLocator());
    ASSERT_EQ(Locator(locator3.toString()), buildedDocs[2]->GetLocator());
}

ACTION_P(SetFatalError, p) {
    p->setFatalError();
    return true;
}

TEST_F(RawDocBuilderConsumerTest, testFatalError) {
    RawDocumentPtr rawDoc = createDoc(common::Locator(0));
    StrictMockBuilder builder;
    RawDocBuilderConsumer consumer(&builder);
    EXPECT_CALL(builder, doBuild(_))
        .WillOnce(Return(true))
        .WillOnce(SetFatalError(&builder));
    EXPECT_EQ(FE_OK, consumer.consume(rawDoc));
    EXPECT_EQ(FE_FATAL, consumer.consume(rawDoc));
}

}
}
