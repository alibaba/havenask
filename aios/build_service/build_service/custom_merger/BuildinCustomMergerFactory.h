#ifndef ISEARCH_BS_BUILDINCUSTOMMERGERFACTORY_H
#define ISEARCH_BS_BUILDINCUSTOMMERGERFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMergerFactory.h"

namespace build_service {
namespace custom_merger {

class BuildinCustomMergerFactory : public CustomMergerFactory
{
public:
    BuildinCustomMergerFactory();
    ~BuildinCustomMergerFactory();
private:
    BuildinCustomMergerFactory(const BuildinCustomMergerFactory &);
    BuildinCustomMergerFactory& operator=(const BuildinCustomMergerFactory &);
public:
    bool init(const KeyValueMap &parameters) override {
        return true;
    }
    void destroy() override {
        delete this;
    }
    CustomMerger* createCustomMerger(const std::string &mergerName) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildinCustomMergerFactory);

}
}

#endif //ISEARCH_BS_BUILDINCUSTOMMERGERFACTORY_H
