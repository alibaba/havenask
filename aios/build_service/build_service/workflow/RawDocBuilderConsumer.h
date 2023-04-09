#ifndef ISEARCH_BS_RAWDOCBUILDERCONSUMER_H
#define ISEARCH_BS_RAWDOCBUILDERCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include "build_service/processor/Processor.h"
#include "build_service/workflow/Consumer.h"

namespace build_service {
namespace workflow {

class RawDocBuilderConsumer : public RawDocConsumer
{
public:
    RawDocBuilderConsumer(builder::Builder *builder,
                          processor::Processor *processor = nullptr);
    ~RawDocBuilderConsumer();
private:
    RawDocBuilderConsumer(const RawDocBuilderConsumer &);
    RawDocBuilderConsumer& operator=(const RawDocBuilderConsumer &);
public:
    /* override */ FlowError consume(const document::RawDocumentPtr &item);
    /* override */ bool getLocator(common::Locator &locator) const;
    /* override */ bool stop(FlowError lastError);

public:
    void SetEndBuildTimestamp(int64_t endBuildTs) {
        _endTimestamp = endBuildTs;
    }

private:
    FlowError processAndBuildDoc(const document::RawDocumentPtr &item);
    FlowError buildDoc(const document::RawDocumentPtr &item);

private:
    builder::Builder *_builder;
    processor::Processor *_processor;
    volatile int64_t _endTimestamp;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RawDocBuilderConsumer);

}
}

#endif //ISEARCH_BS_RAWDOCBUILDERCONSUMER_H
