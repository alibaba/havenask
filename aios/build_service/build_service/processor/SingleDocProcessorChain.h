#ifndef ISEARCH_BS_SINGLEDOCPROCESSORCHAIN_H
#define ISEARCH_BS_SINGLEDOCPROCESSORCHAIN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/plugin/PlugInManager.h"

namespace build_service {
namespace processor {

class DocumentProcessor;
class SingleDocProcessorChain : public DocumentProcessorChain
{
public:
    SingleDocProcessorChain(const plugin::PlugInManagerPtr &pluginManagerPtr);
    SingleDocProcessorChain(SingleDocProcessorChain &other);
    ~SingleDocProcessorChain();
public:
    /* override */
    DocumentProcessorChain *clone();

public:
    // for init
    void addDocumentProcessor(DocumentProcessor *processor);
    
protected:
    bool processExtendDocument(
            const document::ExtendDocumentPtr &extendDocument) override;
    
    void batchProcessExtendDocs(
            const std::vector<document::ExtendDocumentPtr>& extDocVec) override;
    
public:
    // for test
    const std::vector<DocumentProcessor*>& getDocumentProcessors() {
        return _documentProcessors;
    }

private:
    
private:
    plugin::PlugInManagerPtr _pluginManagerPtr; // hold plugin so for processor.
    std::vector<DocumentProcessor*> _documentProcessors;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleDocProcessorChain);

}
}

#endif //ISEARCH_BS_SINGLEDOCPROCESSORCHAIN_H
