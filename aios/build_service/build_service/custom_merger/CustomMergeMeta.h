#ifndef ISEARCH_BS_CUSTOMMERGEMETA_H
#define ISEARCH_BS_CUSTOMMERGEMETA_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMergePlan.h"
#include <autil/legacy/jsonizable.h>
namespace build_service {
namespace custom_merger {

class CustomMergeMeta : public autil::legacy::Jsonizable
{
public:
    CustomMergeMeta();
    ~CustomMergeMeta();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void addMergePlan(const CustomMergePlan& mergePlan)
    {
        _mergePlans.push_back(mergePlan);
    }
    void store(const std::string& path);
    void load(const std::string& path);
    bool getMergePlan(size_t instanceId, CustomMergePlan& plan) const;
private:
    std::vector<CustomMergePlan> _mergePlans;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergeMeta);

}
}

#endif //ISEARCH_BS_CUSTOMMERGEMETA_H
