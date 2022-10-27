#ifndef ISEARCH_FAKEFILLSUMMAYPROCESSOR_H
#define ISEARCH_FAKEFILLSUMMAYPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class FakeFillSummaryProcessor : public QrsProcessor
{
public:
    FakeFillSummaryProcessor();
    ~FakeFillSummaryProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);

    std::string getName() const
    {
        return "FakeFillSummaryProcessor";
    }

    virtual QrsProcessorPtr clone();
    virtual void fillSummary(const common::RequestPtr &requestPtr,
                             const common::ResultPtr &resultPtr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeFillSummaryProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKEFILLSUMMAYPROCESSOR_H
