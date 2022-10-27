#ifndef ISEARCH_TOKEN_REWRITE_FACTORY_H
#define ISEARCH_TOKEN_REWRITE_FACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsModuleFactory.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsProcessor;

class QrsTokenRewriteProcessorFactory : public QrsModuleFactory
{
public:
    QrsTokenRewriteProcessorFactory();
    virtual ~QrsTokenRewriteProcessorFactory();
public:
    virtual bool init(const KeyValueMap &parameters);
    virtual void destroy();

    virtual QrsProcessor* createQrsProcessor(const std::string &name);
private:
    HA3_LOG_DECLARE();
};

// extern "C" ModuleFactory* createFactory();
// extern "C" void destroyFactory(ModuleFactory *factory);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_FAKEQRSMODULEFACTORY_H
