#ifndef ISEARCH_KEYWORDFILTERPROCESSOR_H
#define ISEARCH_KEYWORDFILTERPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class KeywordFilterProcessor : public QrsProcessor
{
public:
    KeywordFilterProcessor();
    virtual ~KeywordFilterProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
    virtual std::string getName() const;
private:
    HA3_LOG_DECLARE();
};
void replaceString(std::string &oriString, const std::string &from, const std::string &to);
HA3_TYPEDEF_PTR(KeywordFilterProcessor);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_KEYWORDFILTERPROCESSOR_H
