#ifndef ISEARCH_BS_TOKENCOMBINEDDOCUMENTPROCESSOR_H
#define ISEARCH_BS_TOKENCOMBINEDDOCUMENTPROCESSOR_H

#include "build_service/util/Log.h"
#include <set>
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class TokenCombinedDocumentProcessor : public DocumentProcessor
{
public:
    TokenCombinedDocumentProcessor();
    TokenCombinedDocumentProcessor(const TokenCombinedDocumentProcessor &other);
    virtual ~TokenCombinedDocumentProcessor();
public:
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();
    virtual bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader) ;
    virtual std::string getDocumentProcessorName() const {return "TokenCombinedDocumentProcessor";}
public://for test
    void setPrefixStopwordSet(const std::set<std::string> &prefixTokenSet);
private:
    void fillStopTokenSet(const KeyValueMap &kvMap,
                          const std::string &stopTokenIdentifier,
                          std::set<std::string> &stopTokenSet);
    void processSection(document::TokenizeSection* section) const;
    void combinePreToken(document::TokenizeSection::Iterator& preIt,
                         document::TokenizeSection::Iterator& curIt,
                         document::TokenizeSection* section) const;
    void combineNextToken(document::TokenizeSection::Iterator& curIt,
                          document::TokenizeSection::Iterator& nextIt,
                          document::TokenizeSection* section) const;

public:
    static const std::string PREFIX_IDENTIFIER;
    static const std::string SUFFIX_IDENTIFIER;
    static const std::string INFIX_IDENTIFIER;

    static const std::string PROCESSOR_NAME;

    std::set<std::string> _prefixTokenSet;
    std::set<std::string> _suffixTokenSet;
    std::set<std::string> _infixTokenSet;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schemaPtr;

    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_TOKENCOMBINEDDOCUMENTPROCESSOR_H
