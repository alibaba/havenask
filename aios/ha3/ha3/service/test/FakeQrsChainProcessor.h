#ifndef ISEARCH_FAKEQRSCHAINPROCESSOR_H
#define ISEARCH_FAKEQRSCHAINPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(service);

class FakeQrsChainProcessor : public qrs::QrsProcessor
{
public:
    FakeQrsChainProcessor();
    ~FakeQrsChainProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr)
    {
        if (_requestSet) {
            requestPtr = _requestPtr;
        }
        resultPtr = _resultPtr;
    }
    virtual qrs::QrsProcessorPtr clone(){
        qrs::QrsProcessorPtr ptr(new FakeQrsChainProcessor(*this));
        return ptr;
    }
    void setResultPtr(const common::ResultPtr &resultPtr){
        _resultPtr = resultPtr;
    }
    void setRequest(const common::RequestPtr &requestPtr) {
        _requestSet = true;
        _requestPtr = requestPtr;
    }
private:
    bool _requestSet;
    common::RequestPtr _requestPtr;
    common::ResultPtr _resultPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeQrsChainProcessor);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_FAKEQRSCHAINPROCESSOR_H
