#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"

using namespace std;
using namespace build_service::document;
SWIFT_USE_NAMESPACE(common);

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, SwiftFieldFilterRawDocumentParser);

SwiftFieldFilterRawDocumentParser::SwiftFieldFilterRawDocumentParser() {
}

SwiftFieldFilterRawDocumentParser::~SwiftFieldFilterRawDocumentParser() {
}

bool SwiftFieldFilterRawDocumentParser::parse(
        const string &docString, RawDocument &rawDoc) 
{
    if (!_fieldGroupReader.fromProductionString(docString)) {
        string errorMsg = "failed to parse from swift field filter message, data[" + docString + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    size_t fieldCount = _fieldGroupReader.getFieldSize();
    for (size_t i = 0; i < fieldCount; ++i) {
        const Field *field = _fieldGroupReader.getField(i);
        assert(field);
        if (field->isExisted && !field->name.empty()) {
            rawDoc.setField(field->name.c_str(), field->name.size(),
                    field->value.c_str(), field->value.size());
        }
    }

    if (rawDoc.getFieldCount() == 0) {
        string errorMsg = "fieldCount is zero";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (!rawDoc.exist(CMD_TAG)) {
        rawDoc.setField(CMD_TAG, CMD_ADD);
    }

    return true;
}


}
}
