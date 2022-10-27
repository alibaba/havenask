#ifndef ISEARCH_QRSMODULEFACTORYSAMPLE_H
#define ISEARCH_QRSMODULEFACTORYSAMPLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsModuleFactory.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsProcessor;

class QrsModuleFactorySample : public QrsModuleFactory
{
public:
    QrsModuleFactorySample();
    virtual ~QrsModuleFactorySample();
public:
    virtual bool init(const KeyValueMap &parameters);
    virtual void destroy();

    virtual QrsProcessor* createQrsProcessor(const std::string &processorName);
// private:
//     LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSMODULEFACTORYSAMPLE_H
