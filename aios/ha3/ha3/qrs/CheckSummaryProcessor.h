#ifndef ISEARCH_CHECKSUMMARYPROCESSOR_H
#define ISEARCH_CHECKSUMMARYPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class CheckSummaryProcessor : public QrsProcessor
{
public:
    CheckSummaryProcessor();
    ~CheckSummaryProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);

    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "CheckSummaryProcessor";
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CheckSummaryProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_CHECKSUMMARYPROCESSOR_H
