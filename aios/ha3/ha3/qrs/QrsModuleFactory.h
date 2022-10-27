#ifndef ISEARCH_QRSMODULEFACTORY_H
#define ISEARCH_QRSMODULEFACTORY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/ModuleFactory.h>

BEGIN_HA3_NAMESPACE(qrs);
class QrsProcessor;
static const std::string MODULE_FUNC_QRS = "_Qrs";
class QrsModuleFactory : public util::ModuleFactory
{
// public:
//     QrsModuleFactory();
//     virtual ~QrsModuleFactory();
public:
    virtual bool init(const KeyValueMap &parameters) = 0;
    virtual void destroy() = 0;

    virtual QrsProcessor* createQrsProcessor(const std::string &name) = 0;
private:
    HA3_LOG_DECLARE();
};

// extern "C" ModuleFactory* createFactory();
// extern "C" void destroyFactory(ModuleFactory *factory);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSMODULEFACTORY_H
