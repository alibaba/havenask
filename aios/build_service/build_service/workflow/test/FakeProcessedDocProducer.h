#ifndef ISEARCH_BS_FAKEPROCESSEDDOCPRODUCER_H
#define ISEARCH_BS_FAKEPROCESSEDDOCPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"

namespace build_service {
namespace workflow {

class FakeProcessedDocProducer : public SwiftProcessedDocProducer
{
public:
    FakeProcessedDocProducer(uint32_t docCount);
    ~FakeProcessedDocProducer();

private:
    FakeProcessedDocProducer(const FakeProcessedDocProducer &);
    FakeProcessedDocProducer& operator=(const FakeProcessedDocProducer &);

public:
    /*override*/ FlowError produce(document::ProcessedDocumentVecPtr &processedDocVec);
    /*override*/ bool seek(const common::Locator &locator);
    /*override*/ bool stop();
    /*override*/ bool getMaxTimestamp(int64_t &timestamp);

private:
    uint32_t _totalDocCount;
    uint32_t _idx;
    uint32_t _sleep;
private:
    BS_LOG_DECLARE();
};


}
}

#endif //ISEARCH_BS_FAKEPROCESSEDDOCPRODUCER_H
