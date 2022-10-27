#ifndef ISEARCH_COMMUNICATIONQRSMODULEFACTORY_H
#define ISEARCH_COMMUNICATIONQRSMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsModuleFactory.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsProcessor;

class CommunicationQrsModuleFactory : public QrsModuleFactory
{
public:
    CommunicationQrsModuleFactory();
    virtual ~CommunicationQrsModuleFactory();
public:
    /* override */ bool init(const KeyValueMap &parameters);
    /* override */ void destroy();

    /* override */ QrsProcessor* createQrsProcessor(const std::string &processorName);
// private:
//     LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_COMMUNICATIONQRSMODULEFACTORY_H
