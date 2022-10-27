#ifndef ISEARCH_FAKEQRSMODULEFACTORY_H
#define ISEARCH_FAKEQRSMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/QrsModuleFactory.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsProcessor;

class FakeQrsModuleFactory : public QrsModuleFactory
{
public:
    FakeQrsModuleFactory();
    virtual ~FakeQrsModuleFactory();
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
