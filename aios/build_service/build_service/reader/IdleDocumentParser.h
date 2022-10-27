#ifndef ISEARCH_BS_IDLEDOCUMENTPARSER_H
#define ISEARCH_BS_IDLEDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/raw_document_parser.h>
#include <indexlib/document/raw_document.h>

namespace build_service {
namespace reader {

class IdleDocumentParser : public IE_NAMESPACE(document)::RawDocumentParser
{
public:
    IdleDocumentParser(const std::string &fieldName);
    ~IdleDocumentParser();
private:
    IdleDocumentParser(const IdleDocumentParser &);
    IdleDocumentParser& operator=(const IdleDocumentParser &);
public:
    /* override */ bool parse(const std::string &docString,
                              IE_NAMESPACE(document)::RawDocument &rawDoc);
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }    

private:
    std::string _fieldName;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IdleDocumentParser);

}
}

#endif //ISEARCH_BS_IDLEDOCUMENTPARSER_H
