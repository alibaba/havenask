#ifndef ISEARCH_FAKEQRSPROCESSOR_H
#define ISEARCH_FAKEQRSPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class FakeQrsProcessor : public QrsProcessor
{
public:
    FakeQrsProcessor();
    ~FakeQrsProcessor();
public:
    void process(common::RequestPtr &requestPtr, 
                 common::ResultPtr &resultPtr) 
    {
        _requestPtr.reset();
        if (requestPtr) {
            std::string str;
            requestPtr->serializeToString(str);
            _requestPtr.reset(new common::Request);
            _requestPtr->deserializeFromString(str);
        }
        resultPtr = _resultPtr;
    }

    QrsProcessorPtr clone() {
        return QrsProcessorPtr(new FakeQrsProcessor(*this));
    }

public:
    void setRequest(common::RequestPtr requestPtr) {
        _requestPtr = requestPtr;
    }
    common::RequestPtr getRequest() const {
        return _requestPtr;
    }
    void setResult(const common::ResultPtr &resultPtr) {
        _resultPtr = resultPtr;
    }
private:
    common::RequestPtr _requestPtr;
    common::ResultPtr _resultPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeQrsProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKEQRSPROCESSOR_H
