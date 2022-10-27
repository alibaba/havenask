#ifndef ISEARCH_BS_TOKENIZEDOCUMENTPROCESSOR_H
#define ISEARCH_BS_TOKENIZEDOCUMENTPROCESSOR_H

#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/analyzer/Token.h"
#include <indexlib/indexlib.h>
#include <indexlib/config/index_partition_schema.h>

namespace build_service {
namespace analyzer {
class Analyzer;
class AnalyzerFactory;
}
}

namespace build_service {
namespace processor {

class TokenizeDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    TokenizeDocumentProcessor();
    TokenizeDocumentProcessor(const TokenizeDocumentProcessor &other);
    ~TokenizeDocumentProcessor();
public:
    virtual bool process(const document::ExtendDocumentPtr& document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();

    virtual bool init(const DocProcessorInitParam &param);

    virtual std::string getDocumentProcessorName() const
    { return PROCESSOR_NAME; }

private:
    bool checkAnalyzers();

    bool tokenizeTextField(const document::TokenizeFieldPtr &field,
                           const autil::ConstString &fieldValue,
                           const std::string &fieldAnalyzerName);
    bool doTokenizeTextField(const document::TokenizeFieldPtr &field,
                             const autil::ConstString &fieldValue,
                             analyzer::Analyzer *analyzer);
    bool tokenizeMultiValueField(const document::TokenizeFieldPtr &field,
                                 const autil::ConstString &fieldValue);
    bool tokenizeSingleValueField(const document::TokenizeFieldPtr &field,
                                  const autil::ConstString &fieldValue);
    bool extractSectionWeight(document::TokenizeSection *section,
                              const analyzer::Analyzer &analyzer);
    analyzer::Analyzer* getAnalyzer(fieldid_t fieldId,
                                    const std::string &fieldAnalyzerName) const;
    bool findFirstNonDelimiterToken(const analyzer::Analyzer &analyzer,
                                    analyzer::Token &token) const;
private:
    IE_NAMESPACE(config)::IndexSchemaPtr _indexSchema;
    IE_NAMESPACE(config)::FieldSchemaPtr _fieldSchema;
    std::tr1::shared_ptr<analyzer::AnalyzerFactory> _analyzerFactoryPtr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TokenizeDocumentProcessor);

}
}

#endif //ISEARCH_BS_TOKENIZEDOCUMENTPROCESSOR_H
