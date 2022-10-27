#ifndef ISEARCH_BS_CUSTOMMERGERFACTORY_H
#define ISEARCH_BS_CUSTOMMERGERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/custom_merger/CustomMerger.h"

namespace build_service {
namespace custom_merger {

class CustomMergerFactory : public plugin::ModuleFactory
{
public:
    CustomMergerFactory() {}
    virtual ~CustomMergerFactory() {}
private:
    CustomMergerFactory(const CustomMergerFactory &);
    CustomMergerFactory& operator=(const CustomMergerFactory &);
public:
    virtual bool init(const KeyValueMap &parameters) = 0;
    virtual void destroy() = 0;
    virtual CustomMerger* createCustomMerger(const std::string &mergerName) = 0;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerFactory);

}
}

#endif //ISEARCH_BS_CUSTOMMERGERFACTORY_H
