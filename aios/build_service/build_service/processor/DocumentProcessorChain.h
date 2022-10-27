#ifndef ISEARCH_BS_DOCUMENTPROCESSORCHAIN_H
#define ISEARCH_BS_DOCUMENTPROCESSORCHAIN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/ExtendDocument.h"
#include <indexlib/document/document_rewriter/document_rewriter.h>
#include <indexlib/document/document_factory_wrapper.h>
#include <indexlib/document/builtin_parser_init_param.h>
#include <indexlib/document/document.h>

namespace build_service {
namespace processor {

class DocumentProcessorChain
{
public:
    typedef IE_NAMESPACE(document)::DocumentRewriter IndexDocumentRewriter;
    typedef std::tr1::shared_ptr<IndexDocumentRewriter> IndexDocumentRewriterPtr;
    typedef std::vector<IndexDocumentRewriterPtr> IndexDocumentRewriterVector;

public:
    DocumentProcessorChain();
    DocumentProcessorChain(DocumentProcessorChain& other);
    virtual ~DocumentProcessorChain();

public:
    // only return NULL when process failed
    document::ProcessedDocumentPtr process(const document::RawDocumentPtr& rawDoc);

    document::ProcessedDocumentVecPtr batchProcess(
            const document::RawDocumentVecPtr &batchRawDocs);

    virtual DocumentProcessorChain *clone() = 0;

    void setTolerateFieldFormatError(bool tolerateFormatError)
    { _tolerateFormatError = tolerateFormatError; }

    void setDocumentSerializeVersion(uint32_t serializeVersion)
    { _docSerializeVersion = serializeVersion; }

    bool init(const IE_NAMESPACE(document)::DocumentFactoryWrapperPtr& wrapper,
              const IE_NAMESPACE(document)::DocumentInitParamPtr& initParam,
              bool useRawDocAsDoc = false);
public:
    virtual bool processExtendDocument(const document::ExtendDocumentPtr &extendDocument) = 0;
    virtual void batchProcessExtendDocs(
            const std::vector<document::ExtendDocumentPtr>& extDocVec) = 0;

protected:
    document::ProcessedDocumentPtr handleExtendDocument(
            const document::ExtendDocumentPtr &extendDocument);
            
private:
    void handleBuildInProcessorWarnings(
        const document::ExtendDocumentPtr& extendDocument,
        const IE_NAMESPACE(document)::DocumentPtr& indexDocument);
    
private:
    IE_NAMESPACE(document)::DocumentFactoryWrapperPtr _docFactoryWrapper;
    IE_NAMESPACE(document)::DocumentParserPtr _docParser;
    uint32_t _docSerializeVersion;
    bool _tolerateFormatError;
    bool _useRawDocAsDoc;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentProcessorChain);
typedef std::vector<DocumentProcessorChainPtr> DocumentProcessorChainVec;
BS_TYPEDEF_PTR(DocumentProcessorChainVec);


}
}

#endif //ISEARCH_BS_DOCUMENTPROCESSORCHAIN_H
