#ifndef ISEARCH_BS_RAW_DOCUMENT_H
#define ISEARCH_BS_RAW_DOCUMENT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/common/Locator.h"
#include "build_service/document/RawDocumentHashMapManager.h"
#include "build_service/document/RawDocumentHashMap.h"
#include <indexlib/document/raw_document.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::RawDocument RawDocument;
BS_TYPEDEF_PTR(RawDocument);

typedef std::vector<RawDocumentPtr> RawDocumentVec;
BS_TYPEDEF_PTR(RawDocumentVec);

}
}

#endif //BUILD_RAW_DOCUMENT_H
