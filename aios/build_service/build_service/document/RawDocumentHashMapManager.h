#ifndef ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H
#define ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/raw_document/key_map_manager.h>

namespace build_service {
namespace document {

typedef IE_NAMESPACE(document)::KeyMapManager RawDocumentHashMapManager;
BS_TYPEDEF_PTR(RawDocumentHashMapManager);

}
}

#endif //ISEARCH_BS_RAWDOCUMENTHASHMAPMANAGER_H
