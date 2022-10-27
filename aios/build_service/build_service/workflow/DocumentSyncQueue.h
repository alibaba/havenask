#ifndef ISEARCH_BS_DOCUMENTSYNCQUEUE_H
#define ISEARCH_BS_DOCUMENTSYNCQUEUE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/SynchronizedQueue.h>
#include <indexlib/document/document.h>
namespace build_service {
namespace workflow {

typedef autil::SynchronizedQueue<IE_NAMESPACE(document)::DocumentPtr> DocumentSyncQueue;
BS_TYPEDEF_PTR(DocumentSyncQueue);

typedef std::map<std::string, DocumentSyncQueuePtr> ClusterDocSyncQueues;

}
}

#endif //ISEARCH_BS_DOCUMENTSYNCQUEUE_H
