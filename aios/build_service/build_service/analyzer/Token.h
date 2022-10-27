#ifndef ISEARCH_BS_TOKEN_H
#define ISEARCH_BS_TOKEN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/extend_document/tokenize/analyzer_token.h>

namespace build_service {
namespace analyzer {

typedef IE_NAMESPACE(document)::AnalyzerToken Token;
typedef std::tr1::shared_ptr<Token> TokenPtr;

}
}

#endif //ISEARCH_BS_TOKEN_H
