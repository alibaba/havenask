#ifndef ISEARCH_FAKEREQUESTPROCESSOR_H
#define ISEARCH_FAKEREQUESTPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class FakeRequestProcessor : public QrsProcessor
{
public:
    FakeRequestProcessor();
    virtual ~FakeRequestProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "FakeRequestProcessor";
    }
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKEREQUESTPROCESSOR_H
