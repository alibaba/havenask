#ifndef ISEARCH_BS_MAINSUBDOCPROCESSORCHAIN_H
#define ISEARCH_BS_MAINSUBDOCPROCESSORCHAIN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/SingleDocProcessorChain.h"

namespace build_service {
namespace processor {

class MainSubDocProcessorChain : public DocumentProcessorChain
{
public:
    MainSubDocProcessorChain(DocumentProcessorChain *mainDocProcessor,
                             DocumentProcessorChain *subDocProcessor);
    MainSubDocProcessorChain(MainSubDocProcessorChain &other);
    ~MainSubDocProcessorChain();
public:
    /* override */ DocumentProcessorChain *clone();
    
protected:
    bool processExtendDocument(
            const document::ExtendDocumentPtr &extendDocument) override;

    void batchProcessExtendDocs(
            const std::vector<document::ExtendDocumentPtr>& extDocVec) override;

private:
    DocumentProcessorChain *_mainDocProcessor;
    DocumentProcessorChain *_subDocProcessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MainSubDocProcessorChain);

}
}

#endif //ISEARCH_BS_MAINSUBDOCPROCESSORCHAIN_H
