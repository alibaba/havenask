#ifndef ISEARCH_KEYWORDREPLACEPROCESSOR_H
#define ISEARCH_KEYWORDREPLACEPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/qrs/test/KeywordFilterProcessor.h>
BEGIN_HA3_NAMESPACE(qrs);

class KeywordReplaceProcessor : public QrsProcessor
{
public:
    KeywordReplaceProcessor();
    virtual ~KeywordReplaceProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
    virtual std::string getName() const;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(KeywordReplaceProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_KEYWORDREPLACEPROCESSOR_H
