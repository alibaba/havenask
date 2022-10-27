#ifndef ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H
#define ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/RawDocumentParser.h"
#include <swift/common/FieldGroupReader.h>

namespace build_service {
namespace reader {

class SwiftFieldFilterRawDocumentParser : public RawDocumentParser {
public:
    SwiftFieldFilterRawDocumentParser();
~SwiftFieldFilterRawDocumentParser();
private:
    SwiftFieldFilterRawDocumentParser(const SwiftFieldFilterRawDocumentParser &);
    SwiftFieldFilterRawDocumentParser& operator=(const SwiftFieldFilterRawDocumentParser &);
public:
    virtual bool parse(const std::string &docString, document::RawDocument &rawDoc);
        bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }    
private:
    SWIFT_NS(common)::FieldGroupReader _fieldGroupReader;
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H
