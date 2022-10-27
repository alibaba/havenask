#ifndef ISEARCH_QRSPROCESSORSAMPLE_H
#define ISEARCH_QRSPROCESSORSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsProcessor.h>

BEGIN_HA3_NAMESPACE(qrs);

class QrsProcessorSample : public QrsProcessor
{
public:
    QrsProcessorSample();
    virtual ~QrsProcessorSample();
public:
    virtual void process(common::RequestPtr &requestPtr, common::ResultPtr &resultPtr);

    /** attention: you should assure the cloned 'QrsProcessor' is thread-safe. */
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "QrsProcessorSample";
    }
// private:
//     LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSPROCESSORSAMPLE_H
