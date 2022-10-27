#ifndef ISEARCH_REQUESTPARSERPROCESSOR_H
#define ISEARCH_REQUESTPARSERPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/config/QueryInfo.h>

BEGIN_HA3_NAMESPACE(qrs);

class RequestParserProcessor : public QrsProcessor
{
public:
    RequestParserProcessor(const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    virtual ~RequestParserProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "RequestParserProcessor";
    }
private:
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_REQUESTPARSERPROCESSOR_H
