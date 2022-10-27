#include "build_service/test/unittest.h"
#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/config/ResourceReader.h"
#include <autil/ConstString.h>
#include <functional>
#include <indexlib/document/raw_document/default_raw_document.h>
#include <indexlib/config/index_partition_schema.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {

class SingleDocProcessorChainTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    document::ExtendDocumentPtr createExtendDocWithCmd(DocOperateType type);
    static bool updateCmd(const ExtendDocumentPtr &document, DocOperateType type);
    bool processExtendDocument(DocumentProcessorChain *chain,
                               const ExtendDocumentPtr &extendDocument)
    {
        return chain->processExtendDocument(extendDocument);
    }
protected:
    plugin::PlugInManagerPtr _plugInManagerPtr;

};

class MockDocumentProcessor : public DocumentProcessor {
public:
    MockDocumentProcessor(int docOperateFlag = ADD_DOC)
        : DocumentProcessor(docOperateFlag)
    {
    }
    ~MockDocumentProcessor() {}
public:
    MOCK_METHOD1(process, bool(const ExtendDocumentPtr &document));
    MOCK_METHOD0(clone, DocumentProcessor*());

public:
    DocumentProcessor* cloneForInvoke() {
        return new MockDocumentProcessor();
    }
};

typedef ::testing::StrictMock<MockDocumentProcessor> StrictMockDocumentProcessor;

void SingleDocProcessorChainTest::setUp() {
}

void SingleDocProcessorChainTest::tearDown() {
}

TEST_F(SingleDocProcessorChainTest, testSimpleProcess) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(ADD_DOC);
        ON_CALL(*processor, process(_)).WillByDefault(Return(true));
        EXPECT_CALL(*processor, process(_));
        chain.addDocumentProcessor(processor);
    }
    ExtendDocumentPtr extendDocPtr;
    extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
}

TEST_F(SingleDocProcessorChainTest, testClone) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor();
        ON_CALL(*processor, clone()).WillByDefault(
                Invoke(std::bind(&MockDocumentProcessor::cloneForInvoke, processor)));
        EXPECT_CALL(*processor, clone());
        chain.addDocumentProcessor(processor);
    }

    string configRoot = TEST_DATA_PATH"document_processor_chain_creator_test/create_success/";
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    IndexPartitionSchemaPtr schema =
        resourceReaderPtr->getSchemaBySchemaTableName("simple_table");
    DocumentFactoryWrapperPtr wrapper(new DocumentFactoryWrapper(schema));
    wrapper->Init();
    chain.init(wrapper, DocumentInitParamPtr());
    ASSERT_TRUE(chain._docFactoryWrapper.get());
    ASSERT_TRUE(chain._docParser.get());

    unique_ptr<DocumentProcessorChain> clonedChain(chain.clone());
    SingleDocProcessorChain *clonedSingleChain =
        ASSERT_CAST_AND_RETURN(SingleDocProcessorChain, clonedChain.get());
    ASSERT_TRUE(clonedSingleChain->_docFactoryWrapper.get());
    ASSERT_TRUE(clonedSingleChain->_docParser.get());
}

TEST_F(SingleDocProcessorChainTest, testProcessException) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(ADD_DOC);
        bool returnValue = true;
        if (i == 2) {
            returnValue = false;
        }
        ON_CALL(*processor, process(_)).WillByDefault(Return(returnValue));
        if (i <= 2) {
            EXPECT_CALL(*processor, process(_));
        }
        chain.addDocumentProcessor(processor);
    }
    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_FALSE(processExtendDocument(&chain, extendDocPtr));
}

TEST_F(SingleDocProcessorChainTest, testProcessSkip) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(ADD_DOC);
        if (i < 2) {
            EXPECT_CALL(*processor, process(_));
            ON_CALL(*processor, process(_)).WillByDefault(Return(true));
        } else if (i == 2) {
            EXPECT_CALL(*processor, process(_));
            ON_CALL(*processor, process(_)).WillByDefault(
                    Invoke(std::bind(&SingleDocProcessorChainTest::updateCmd,
                                    std::placeholders::_1, SKIP_DOC)));
        }
        chain.addDocumentProcessor(processor);
    }
    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
}

TEST_F(SingleDocProcessorChainTest, testProcessUnknown) {
    {
        SingleDocProcessorChain chain(_plugInManagerPtr);
        for (size_t i = 0; i < 5; ++i) {
            StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(ADD_DOC);
            EXPECT_CALL(*processor, process(_));
            if (i != 2) {
                ON_CALL(*processor, process(_)).WillByDefault(Return(true));
            } if (i == 2) {
                ON_CALL(*processor, process(_)).WillByDefault(
                        Invoke(std::bind(&SingleDocProcessorChainTest::updateCmd, std::placeholders::_1, UNKNOWN_OP)));
            }
            chain.addDocumentProcessor(processor);
        }
        ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
        ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
    }
    {
        SingleDocProcessorChain chain(_plugInManagerPtr);
        for (size_t i = 0; i < 5; ++i) {
            StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(ADD_DOC);
            chain.addDocumentProcessor(processor);
        }
        ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(UNKNOWN_OP);
        string cmd = document::CMD_TAG;
        string unknown = "unknown";
        extendDocPtr->getRawDocument()->setField(cmd, unknown);
        ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
    }
}

TEST_F(SingleDocProcessorChainTest, testProcessDocFlag) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    int operateFlags[] = {
        ADD_DOC,
        ADD_DOC | UPDATE_FIELD,
        UPDATE_FIELD,
        ADD_DOC | DELETE_DOC,
        ADD_DOC | UPDATE_FIELD
    };
    bool expectCalls[] = {true, true, false, true, true};
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(operateFlags[i]);
        ON_CALL(*processor, process(_)).WillByDefault(Return(true));
        if (expectCalls[i]) {
            EXPECT_CALL(*processor, process(_));
        }
        chain.addDocumentProcessor(processor);
    }
    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
}

TEST_F(SingleDocProcessorChainTest, testProcessDocFlagFailWithDeleteSub) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(
            DELETE_SUB_DOC);
    ON_CALL(*processor, process(_)).WillByDefault(Return(true));
    EXPECT_CALL(*processor, process(_))
        .Times(0);
    chain.addDocumentProcessor(processor);

    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));

    ExtendDocumentPtr extendDocPtr2 = createExtendDocWithCmd(DELETE_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr2));

    ExtendDocumentPtr extendDocPtr3 = createExtendDocWithCmd(UPDATE_FIELD);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr3));

    ExtendDocumentPtr extendDocPtr4 = createExtendDocWithCmd(SKIP_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr4));
}

TEST_F(SingleDocProcessorChainTest, testProcessDocFlagWithDeleteSub) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(
            DELETE_SUB_DOC);
    ON_CALL(*processor, process(_)).WillByDefault(Return(true));
    EXPECT_CALL(*processor, process(_))
        .Times(1);
    chain.addDocumentProcessor(processor);

    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(DELETE_SUB_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
}

TEST_F(SingleDocProcessorChainTest, testProcessDocFlagChangeCmd) {
    SingleDocProcessorChain chain(_plugInManagerPtr);
    int operateFlags[] = {
        ADD_DOC,
        ADD_DOC | UPDATE_FIELD,
        UPDATE_FIELD,
        ADD_DOC | DELETE_DOC,
        ADD_DOC | UPDATE_FIELD
    };
    bool expectCalls[] = {true, true, true, false, true};
    for (size_t i = 0; i < 5; ++i) {
        StrictMockDocumentProcessor *processor = new StrictMockDocumentProcessor(operateFlags[i]);
        if (i == 1) {
            ON_CALL(*processor, process(_)).WillByDefault(Return(true));
        } else {
            ON_CALL(*processor, process(_)).WillByDefault(
                    Invoke(std::bind(&SingleDocProcessorChainTest::updateCmd, std::placeholders::_1, UPDATE_FIELD)));
        }
        if (expectCalls[i]) {
            EXPECT_CALL(*processor, process(_));
        }
        chain.addDocumentProcessor(processor);
    }
    ExtendDocumentPtr extendDocPtr = createExtendDocWithCmd(ADD_DOC);
    ASSERT_TRUE(processExtendDocument(&chain, extendDocPtr));
}

ExtendDocumentPtr SingleDocProcessorChainTest::createExtendDocWithCmd(DocOperateType type) {
    ExtendDocumentPtr extendDocPtr(new ExtendDocument);
    RawDocumentPtr rawDocumentPtr(new DefaultRawDocument());
    rawDocumentPtr->setDocOperateType(type);
    extendDocPtr->setRawDocument(rawDocumentPtr);
    return extendDocPtr;
}

bool SingleDocProcessorChainTest::updateCmd(const ExtendDocumentPtr &document, DocOperateType type) {
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    rawDoc->setDocOperateType(type);
    return true;
}


}
}
