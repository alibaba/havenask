#ifndef ISEARCH_BS_FAKEALTERFIELDMERGERFACTORY_H
#define ISEARCH_BS_FAKEALTERFIELDMERGERFACTORY_H

#include "build_service/custom_merger/CustomMergerFactory.h"

namespace build_service {
namespace task_base {

class FakeAlterFieldMergerFactory : public custom_merger::CustomMergerFactory
{
public:
    FakeAlterFieldMergerFactory();
    ~FakeAlterFieldMergerFactory();
private:
    FakeAlterFieldMergerFactory(const FakeAlterFieldMergerFactory &);
    FakeAlterFieldMergerFactory& operator=(const FakeAlterFieldMergerFactory &);
public:
    bool init(const KeyValueMap &parameters) override{ return true; }
    void destroy() override{
        delete this;
    }
    custom_merger::CustomMerger* createCustomMerger(
            const std::string &mergerName) override;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeAlterFieldMergerFactory);

}
}

#endif //ISEARCH_BS_FAKEALTERFIELDMERGERFACTORY_H
