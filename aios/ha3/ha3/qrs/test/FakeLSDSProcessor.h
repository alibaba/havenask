#ifndef ISEARCH_FAKELSDSPROCESSOR_H
#define ISEARCH_FAKELSDSPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class FakeLSDSProcessor : public QrsProcessor
{
public:
    FakeLSDSProcessor();
    virtual ~FakeLSDSProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    std::string getName() const
    {
        return "FakeLSDSProcessor";
    }
    virtual QrsProcessorPtr clone();
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKELSDSPROCESSOR_H
