#ifndef ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H
#define ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/raw_document_parser.h>
#include <indexlib/document/raw_document.h>

namespace build_service {
namespace reader {

class CombinedRawDocumentParser : public IE_NAMESPACE(document)::RawDocumentParser
{
public:
    CombinedRawDocumentParser(const std::vector<IE_NAMESPACE(document)::RawDocumentParserPtr>& parsers)
        : _parsers(parsers)
        , _lastParser(0)
        , _parserCount(parsers.size())
    {} 
    ~CombinedRawDocumentParser() {}
private:
    CombinedRawDocumentParser(const CombinedRawDocumentParser &);
    CombinedRawDocumentParser& operator=(const CombinedRawDocumentParser &);
public:
    bool parse(const std::string &docString,
               IE_NAMESPACE(document)::RawDocument &rawDoc);
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }

private:
    std::vector<IE_NAMESPACE(document)::RawDocumentParserPtr> _parsers;
    size_t _lastParser;
    size_t _parserCount;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CombinedRawDocumentParser);

}
}

#endif //ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H
