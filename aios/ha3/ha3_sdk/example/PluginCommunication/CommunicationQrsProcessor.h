#ifndef ISEARCH_QRSPROCESSORSAMPLE_H
#define ISEARCH_QRSPROCESSORSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class CommunicationQrsProcessor : public QrsProcessor
{
public:
    CommunicationQrsProcessor() {}
    virtual ~CommunicationQrsProcessor() {}
public:
    /* override */ void process(common::RequestPtr &requestPtr, common::ResultPtr &resultPtr);
    /* override */ QrsProcessorPtr clone() { return QrsProcessorPtr(new CommunicationQrsProcessor(*this)); }
private:
    bool doDedup(common::ResultPtr &resultPtr);

    void addMinPostageToMetaHits(common::ResultPtr &resultPtr);
public:
    std::string getName() const {
        return "CommunicationQrsProcessor";
    }
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSPROCESSORSAMPLE_H
