#ifndef ISEARCH_FAKERAWSTRINGPROCESSOR_H
#define ISEARCH_FAKERAWSTRINGPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <string>

BEGIN_HA3_NAMESPACE(qrs);

class FakeRawStringProcessor : public QrsProcessor
{
public:
    FakeRawStringProcessor(const std::string &retString);
    ~FakeRawStringProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);

    QrsProcessorPtr clone();

    std::string getName() const {
        return "FakeRawStringProcessor";
    }
private:
    std::string _retString;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKERAWSTRINGPROCESSOR_H
