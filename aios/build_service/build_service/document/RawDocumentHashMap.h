#ifndef ISEARCH_BS_RAWDOCUMENTHASHMAP_H
#define ISEARCH_BS_RAWDOCUMENTHASHMAP_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/raw_document/key_map.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::KeyMap RawDocumentHashMap;
BS_TYPEDEF_PTR(RawDocumentHashMap);

}
}

#endif //ISEARCH_BS_RAWDOCUMENTHASHMAP_H
