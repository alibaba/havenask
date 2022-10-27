#ifndef ISEARCH_BS_MOCKDOCUMENTPROCESSORCHAIN_H
#define ISEARCH_BS_MOCKDOCUMENTPROCESSORCHAIN_H

#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "build_service/test/test.h"
#include "build_service/processor/DocumentProcessorChain.h"

namespace build_service {
namespace processor {

class MockDocumentProcessorChain : public DocumentProcessorChain
{
public:
    MockDocumentProcessorChain() {}
    ~MockDocumentProcessorChain() {}

public:
    MOCK_METHOD0(clone, DocumentProcessorChain*());
    MOCK_METHOD1(processExtendDocument, bool(const document::ExtendDocumentPtr&));
    MOCK_METHOD1(batchProcessExtendDocs, void(
                    const std::vector<document::ExtendDocumentPtr>& extDocVec));
};

typedef ::testing::StrictMock<MockDocumentProcessorChain> StrictMockDocumentProcessorChain;
typedef ::testing::NiceMock<MockDocumentProcessorChain> NiceMockDocumentProcessorChain;

}
}

#endif //ISEARCH_BS_MOCKDOCUMENTPROCESSORCHAIN_H
