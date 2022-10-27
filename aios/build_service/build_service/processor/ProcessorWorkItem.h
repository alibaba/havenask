#ifndef ISEARCH_BS_PROCESSORWORKITEM_H
#define ISEARCH_BS_PROCESSORWORKITEM_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/WorkItem.h>
#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/processor/ProcessorMetricReporter.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"

namespace build_service {
namespace processor {

class ProcessorWorkItem : public autil::WorkItem
{
public:
    ProcessorWorkItem(const DocumentProcessorChainVecPtr &chains,
                      const ProcessorChainSelectorPtr &chainSelector,
                      ProcessorMetricReporter *reporter);
    ~ProcessorWorkItem();
private:
    ProcessorWorkItem(const ProcessorWorkItem &);
    ProcessorWorkItem& operator=(const ProcessorWorkItem &);
public:
    void initProcessData(const document::RawDocumentPtr &rawDocPtr);
    void initBatchProcessData(const document::RawDocumentVecPtr &rawDocsPtr);
    
    // doc will be processed by all chains, error in one chain will not affect other chains
    // doc with *SKIP* and *UNKNOWN* will be ignored, not put in ProcessedDocumentVecPtr
    const document::ProcessedDocumentVecPtr &getProcessedDocs() const;
public:
    /* override */ void process();
    /* override */ void destroy();
    /* override */ void drop();
    int64_t getProcessErrorCount() const { return _processErrorCount; }
    uint32_t getRawDocCount() const;
private:
    void singleProcess();
    void batchProcess();
    
    size_t getRawDocumentsForEachChain(
            const document::RawDocumentVecPtr& batchRawDocsPtr,
            std::vector<document::RawDocumentVecPtr> &rawDocsForChains,
            std::vector<const ProcessorChainSelector::ChainIdVector*> &chainIdQueue);

    void assembleProcessedDoc(size_t chainId,
                              const document::RawDocumentPtr &rawDoc,
                              document::ProcessedDocumentPtr &pDoc);

    void batchProcessWithCustomizeChainSelector();
    void batchProcessWithAllChainSelector();
    
private:
    // input
    document::RawDocumentPtr _rawDocPtr;
    document::RawDocumentVecPtr _batchRawDocsPtr;

    // output
    document::ProcessedDocumentVecPtr _processedDocumentVecPtr;

    // const
    DocumentProcessorChainVecPtr _chains;
    ProcessorChainSelectorPtr _chainSelector;
    ProcessorMetricReporter *_reporter;
    int64_t _processErrorCount;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorWorkItem);
////////////////////////////////////////////////////////////

inline void ProcessorWorkItem::initProcessData(const document::RawDocumentPtr &rawDocPtr)
{
    _rawDocPtr = rawDocPtr;
}


inline void ProcessorWorkItem::initBatchProcessData(const document::RawDocumentVecPtr &rawDocsPtr)
{
    _batchRawDocsPtr = rawDocsPtr;
}

inline const document::ProcessedDocumentVecPtr &ProcessorWorkItem::getProcessedDocs() const {
    return _processedDocumentVecPtr;
}

}
}

#endif //ISEARCH_BS_PROCESSORWORKITEM_H
