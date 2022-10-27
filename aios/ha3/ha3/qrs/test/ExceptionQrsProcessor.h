#ifndef ISEARCH_EXCEPTIONQRSPROCESSOR_H
#define ISEARCH_EXCEPTIONQRSPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class ExceptionQrsProcessor : public QrsProcessor
{
public:
    ExceptionQrsProcessor() {}
    ~ExceptionQrsProcessor() {}
public:
    /* override */ void process(common::RequestPtr &requestPtr, 
                                common::ResultPtr &resultPtr) 
    {
        throw "Exception";
    }

    /* override */ QrsProcessorPtr clone() {
        return QrsProcessorPtr(new ExceptionQrsProcessor(*this));
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ExceptionQrsProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_EXCEPTIONQRSPROCESSOR_H
