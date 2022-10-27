#include "build_service/test/unittest.h"
#include "build_service/processor/MainSubDocProcessorChain.h"
#include "build_service/processor/test/MockDocumentProcessorChain.h"
#include <autil/StringUtil.h>
#include <tr1/functional>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace build_service::document;

namespace build_service {
namespace processor {

class MainSubDocProcessorChainTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    static bool processSubDoc(const document::ExtendDocumentPtr &extendDoc,
                              const std::set<std::string> &failDocs);

    document::ExtendDocumentPtr createExtendDocWithCmd(uint32_t subDocCount);
    bool processExtendDocument(DocumentProcessorChain *chain,
                               const ExtendDocumentPtr &extendDocument)
    {
        return chain->processExtendDocument(extendDocument);
    }
};

void MainSubDocProcessorChainTest::setUp() {
}

void MainSubDocProcessorChainTest::tearDown() {
}

TEST_F(MainSubDocProcessorChainTest, testSimpleProcess) {
    StrictMockDocumentProcessorChain *mainChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*mainChain, processExtendDocument(_)).WillByDefault(Return(true));
    EXPECT_CALL(*mainChain, processExtendDocument(_)).Times(1);

    StrictMockDocumentProcessorChain *subChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*subChain, processExtendDocument(_)).WillByDefault(Return(true));
    EXPECT_CALL(*subChain, processExtendDocument(_)).Times(5);

    MainSubDocProcessorChain chain(mainChain, subChain);
    ExtendDocumentPtr extendDoc = createExtendDocWithCmd(5);
    ASSERT_TRUE(processExtendDocument(&chain, extendDoc));
}

TEST_F(MainSubDocProcessorChainTest, testProcessMainDocFailed) {
    StrictMockDocumentProcessorChain *mainChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*mainChain, processExtendDocument(_)).WillByDefault(Return(false));
    EXPECT_CALL(*mainChain, processExtendDocument(_)).Times(1);

    StrictMockDocumentProcessorChain *subChain = new StrictMockDocumentProcessorChain;

    MainSubDocProcessorChain chain(mainChain, subChain);
    ExtendDocumentPtr extendDoc = createExtendDocWithCmd(5);
    ASSERT_FALSE(processExtendDocument(&chain, extendDoc));
}

TEST_F(MainSubDocProcessorChainTest, testProcessSubDocFailed) {
    StrictMockDocumentProcessorChain *mainChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*mainChain, processExtendDocument(_)).WillByDefault(Return(true));
    EXPECT_CALL(*mainChain, processExtendDocument(_)).Times(1);

    set<string> failDocs;
    failDocs.insert("doc_0");
    failDocs.insert("doc_3");

    StrictMockDocumentProcessorChain *subChain = new StrictMockDocumentProcessorChain;
    ON_CALL(*subChain, processExtendDocument(_)).WillByDefault(
            Invoke(std::bind(&processSubDoc, tr1::placeholders::_1, failDocs)));
    EXPECT_CALL(*subChain, processExtendDocument(_)).Times(5);

    MainSubDocProcessorChain chain(mainChain, subChain);
    ExtendDocumentPtr extendDoc = createExtendDocWithCmd(5);
    ASSERT_TRUE(processExtendDocument(&chain, extendDoc));

    set<string> leftDocs;
    leftDocs.insert("doc_1");
    leftDocs.insert("doc_2");
    leftDocs.insert("doc_4");

    EXPECT_EQ(size_t(3), extendDoc->getSubDocumentsCount());
    for (size_t i = 0; i < extendDoc->getSubDocumentsCount(); ++i) {
        string rawStr(extendDoc->getSubDocument(i)->getRawDocument()->getField("rawDocStr"));
        EXPECT_THAT(leftDocs, ::testing::Contains(rawStr));
    }
}

TEST_F(MainSubDocProcessorChainTest, testClone) {
    StrictMockDocumentProcessorChain *mainChain = new StrictMockDocumentProcessorChain;
    EXPECT_CALL(*mainChain, clone()).Times(1);

    StrictMockDocumentProcessorChain *subChain = new StrictMockDocumentProcessorChain;
    EXPECT_CALL(*subChain, clone()).Times(1);

    DocumentProcessorChain::IndexDocumentRewriterVector docRewriters(2);
    MainSubDocProcessorChain chain(mainChain, subChain);
    chain.setDocumentSerializeVersion(7);
    chain.setTolerateFieldFormatError(false);

    unique_ptr<DocumentProcessorChain> clonedChain(chain.clone());
    ASSERT_EQ((uint32_t)7, clonedChain->_docSerializeVersion);
    ASSERT_FALSE(clonedChain->_tolerateFormatError);
    
    ASSERT_CAST_AND_RETURN(MainSubDocProcessorChain, clonedChain.get());
}

bool MainSubDocProcessorChainTest::processSubDoc(
        const ExtendDocumentPtr &extendDoc, const set<string> &failDocs)
{
    string rawStr(extendDoc->getRawDocument()->getField("rawDocStr"));
    if (failDocs.find(rawStr) != failDocs.end()) {
        return false;
    }
    return true;
}

ExtendDocumentPtr MainSubDocProcessorChainTest::createExtendDocWithCmd(uint32_t subDocCount) {
    ExtendDocumentPtr extendDocPtr(new ExtendDocument);
    for (uint32_t i = 0; i < subDocCount; ++i) {
        ExtendDocumentPtr subExtendDocPtr(new ExtendDocument);
        RawDocumentPtr rawDocumentPtr(new IE_NAMESPACE(document)::DefaultRawDocument());
        string rawDocStr = string("doc_") + autil::StringUtil::toString(i);
        rawDocumentPtr->setField("rawDocStr", rawDocStr);
        subExtendDocPtr->setRawDocument(rawDocumentPtr);
        extendDocPtr->addSubDocument(subExtendDocPtr);
    }
    return extendDocPtr;
}

}
}
