#include "build_service/reader/IdleDocumentParser.h"
#include "build_service/document/DocumentDefine.h"

using namespace std;
using namespace build_service::document;

namespace build_service {
namespace reader {
BS_LOG_SETUP(reader, IdleDocumentParser);

IdleDocumentParser::IdleDocumentParser(const string &fieldName)
    : _fieldName(fieldName)
{
}

IdleDocumentParser::~IdleDocumentParser() {
}

bool IdleDocumentParser::parse(const string &docString,
                               IE_NAMESPACE(document)::RawDocument &rawDoc)
{
    rawDoc.setField(_fieldName, docString);
    rawDoc.setField(CMD_TAG, CMD_ADD);
    return true;
}

}
}
