#include "build_service/test/unittest.h"
#include "build_service/builder/test/MockBuilder.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/document/test/DocumentTestHelper.h"

using namespace std;
using namespace build_service::builder;
using namespace build_service::document;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);

namespace build_service {
namespace workflow {

class DocBuilderConsumerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    static bool mockBuild(const DocumentPtr &doc, vector<DocumentPtr> *docVec) {
        docVec->push_back(doc);
        return true;
    }
    static ProcessedDocumentVecPtr createDoc(const string &pk, const common::Locator &locator)
    {
        ProcessedDocumentVecPtr processedDocVec(new ProcessedDocumentVec);
        // doc 1
        ProcessedDocumentPtr processedDoc(new ProcessedDocument());
        processedDocVec->push_back(processedDoc);
        FakeDocument fakeDoc1(pk);
        DocumentPtr document = DocumentTestHelper::createDocument(fakeDoc1);
        processedDoc->setDocument(document);
        processedDoc->setLocator(locator);
        return processedDocVec;
    }
};

void DocBuilderConsumerTest::setUp() {
}

void DocBuilderConsumerTest::tearDown() {
}

TEST_F(DocBuilderConsumerTest, testConsume) {
    vector<DocumentPtr> buildedDocs;
    StrictMockBuilder builder;
    DocBuilderConsumer consumer(&builder);
    consumer.SetEndBuildTimestamp(101);
    ON_CALL(builder, doBuild(_)).WillByDefault(
            Invoke(std::bind(&mockBuild, std::placeholders::_1, &buildedDocs)));
    EXPECT_CALL(builder, doBuild(_)).Times(3);
    EXPECT_CALL(builder, stop(101)).Times(1);

    ProcessedDocumentVecPtr processedDocVec;

    common::Locator locator1(1);
    processedDocVec = createDoc("aaa", locator1);
    ASSERT_EQ(FE_OK, consumer.consume(processedDocVec));

    common::Locator locator2(2);
    processedDocVec = createDoc("bbb", locator2);
    ASSERT_EQ(FE_OK, consumer.consume(processedDocVec));

    common::Locator locator3(3);
    processedDocVec = createDoc("ccc", locator3);
    ASSERT_EQ(FE_OK, consumer.consume(processedDocVec));

    ASSERT_EQ(size_t(3), buildedDocs.size());

    consumer.stop(FE_EOF);

    DocumentTestHelper::checkDocument(FakeDocument("aaa"), buildedDocs[0]);
    DocumentTestHelper::checkDocument(FakeDocument("bbb"), buildedDocs[1]);
    DocumentTestHelper::checkDocument(FakeDocument("ccc"), buildedDocs[2]);

    ASSERT_EQ(Locator(locator1.toString()), buildedDocs[0]->GetLocator());
    ASSERT_EQ(Locator(locator2.toString()), buildedDocs[1]->GetLocator());
    ASSERT_EQ(Locator(locator3.toString()), buildedDocs[2]->GetLocator());
}

ACTION_P(SetFatalError, p) {
    p->setFatalError();
    return true;
}

TEST_F(DocBuilderConsumerTest, testFatalError) {
    ProcessedDocumentVecPtr doc = createDoc("ccc", common::Locator(0));
    StrictMockBuilder builder;
    DocBuilderConsumer consumer(&builder);
    EXPECT_CALL(builder, doBuild(_))
        .WillOnce(Return(true))
        .WillOnce(SetFatalError(&builder));
    EXPECT_EQ(FE_OK, consumer.consume(doc));
    EXPECT_EQ(FE_FATAL, consumer.consume(doc));
}

}
}
