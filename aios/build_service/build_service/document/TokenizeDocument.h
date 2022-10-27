#ifndef ISEARCH_BS_TOKENIZEDOCUMENT_H
#define ISEARCH_BS_TOKENIZEDOCUMENT_H

#include "build_service/common_define.h"
#include "build_service/document/TokenizeField.h"
#include <indexlib/document/extend_document/tokenize_document.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::TokenizeDocument TokenizeDocument;
BS_TYPEDEF_PTR(TokenizeDocument);

}
}

#endif //ISEARCH_BS_TOKENIZEDOCUMENT_H
