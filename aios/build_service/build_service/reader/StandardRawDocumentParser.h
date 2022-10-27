#ifndef ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H
#define ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentParser.h"
#include "build_service/reader/Separator.h"

namespace build_service {
namespace reader {

class StandardRawDocumentParser : public RawDocumentParser
{
public:
    StandardRawDocumentParser(const std::string &fieldSep,
                              const std::string &keyValueSep);
    ~StandardRawDocumentParser();
private:
    StandardRawDocumentParser(const StandardRawDocumentParser &);
    StandardRawDocumentParser& operator=(const StandardRawDocumentParser &);
public:
    /* override */ bool parse(const std::string &docString,
                              document::RawDocument &rawDoc);
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }    
private:
    void trim(const char * &ptr, size_t &len);
    std::pair<const char*, size_t> findNext(const Separator &sep,
            const char *&docCursor, const char *docEnd);
private:
    Separator _fieldSep;
    Separator _keyValueSep;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(StandardRawDocumentParser);

}
}

#endif //ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H
