#ifndef ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H
#define ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"
#include "build_service/document/ProcessedDocument.h"
#include <autil/Lock.h>

namespace build_service {
namespace local_job {

class ReduceDocumentQueue {
    typedef util::StreamQueue<IE_NAMESPACE(document)::DocumentPtr> ReduceQueue;
public:
    ReduceDocumentQueue(uint16_t reduceCount) {
        for (uint16_t i = 0; i < reduceCount; i++) {
            _reduceQueues.push_back(std::tr1::shared_ptr<ReduceQueue>(new ReduceQueue));
        }
    }
    ReduceDocumentQueue() {
    }

private:
    ReduceDocumentQueue(const ReduceDocumentQueue &);
    ReduceDocumentQueue& operator=(const ReduceDocumentQueue &);

public:
    void push(uint16_t instanceId, const IE_NAMESPACE(document)::DocumentPtr &doc) {
        return _reduceQueues[instanceId]->push(doc);
    }

    bool pop(uint16_t instanceId, IE_NAMESPACE(document)::DocumentPtr &doc) {
        return _reduceQueues[instanceId]->pop(doc);
    }

    void setFinished() {
        for (size_t i = 0; i < _reduceQueues.size(); i++) {
            _reduceQueues[i]->setFinish();
        }
    }

private:
    std::vector<std::tr1::shared_ptr<ReduceQueue> > _reduceQueues;
};

}
}

#endif //ISEARCH_BS_PROCESSEDDOCUMENTRECORD_H
