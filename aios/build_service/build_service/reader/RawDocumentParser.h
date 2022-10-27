#ifndef ISEARCH_BS_RAWDOCUMENTPARSER_H
#define ISEARCH_BS_RAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/RawDocument.h"
#include <indexlib/document/raw_document_parser.h>

namespace build_service {
namespace reader {

typedef IE_NAMESPACE(document)::RawDocumentParser RawDocumentParser;
BS_TYPEDEF_PTR(RawDocumentParser);

}
}

#endif //ISEARCH_BS_RAWDOCUMENTPARSER_H
