#ifndef ISEARCH_BS_DOCBUILDERCONSUMER_H
#define ISEARCH_BS_DOCBUILDERCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/Builder.h"
#include "build_service/workflow/Consumer.h"
namespace build_service {
namespace workflow {

class DocBuilderConsumer : public ProcessedDocConsumer
{
public:
    DocBuilderConsumer(builder::Builder *builder);
    ~DocBuilderConsumer();
private:
    DocBuilderConsumer(const DocBuilderConsumer &);
    DocBuilderConsumer& operator=(const DocBuilderConsumer &);
public:
    /* override */ FlowError consume(const document::ProcessedDocumentVecPtr &item);
    /* override */ bool getLocator(common::Locator &locator) const;
    /* override */ bool stop(FlowError lastError);

public:
    void SetEndBuildTimestamp(int64_t endBuildTs) {
        BS_LOG(INFO, "DocBuilderConsumer will stop at timestamp[%ld]", endBuildTs);
        _endTimestamp = endBuildTs;
    }
        
private:
    builder::Builder *_builder;
    volatile int64_t _endTimestamp;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocBuilderConsumer);

}
}

#endif //ISEARCH_BS_DOCBUILDERCONSUMER_H
