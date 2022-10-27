#ifndef ISEARCH_BS_TOKENIZESECTION_H
#define ISEARCH_BS_TOKENIZESECTION_H

#include "build_service/common_define.h"
#include <indexlib/document/extend_document/tokenize/tokenize_section.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::TokenNode TokenNode;
typedef IE_NAMESPACE(document)::TokenNodeAllocator TokenNodeAllocator;
typedef std::tr1::shared_ptr<TokenNodeAllocator> TokenNodeAllocatorPtr;

typedef IE_NAMESPACE(document)::TokenNodeAllocatorPool TokenNodeAllocatorPool;
typedef IE_NAMESPACE(document)::TokenizeSection TokenizeSection;
BS_TYPEDEF_PTR(TokenizeSection);

}
}

#endif //ISEARCH_BS_TOKENIZESECTION_H
