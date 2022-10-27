#ifndef ISEARCH_BS_TOKENIZEFIELD_H
#define ISEARCH_BS_TOKENIZEFIELD_H

#include "build_service/common_define.h"
#include "build_service/document/TokenizeSection.h"
#include <indexlib/document/extend_document/tokenize/tokenize_field.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::TokenizeField TokenizeField;
BS_TYPEDEF_PTR(TokenizeField);

}
}

#endif //ISEARCH_BS_TOKENIZEFIELD_H
