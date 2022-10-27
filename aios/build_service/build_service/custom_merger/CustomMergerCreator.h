#ifndef ISEARCH_BS_CUSTOMMERGERCREATOR_H
#define ISEARCH_BS_CUSTOMMERGERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/MergePluginConfig.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/MergeResourceProvider.h"

namespace build_service {
namespace custom_merger {

class CustomMergerCreator
{
public:
    CustomMergerCreator();
    ~CustomMergerCreator();
private:
    CustomMergerCreator(const CustomMergerCreator &);
    CustomMergerCreator& operator=(const CustomMergerCreator &);
public:
    bool init(const config::ResourceReaderPtr &resourceReaderPtr,
              IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
    { 
        _resourceReaderPtr = resourceReaderPtr;
        _metricProvider = metricProvider;
        return true;
    }
    CustomMergerPtr create(
                const config::MergePluginConfig &mergeConfig,
                MergeResourceProviderPtr& resourceProvider);
    CustomMerger::CustomMergerInitParam getTestParam();
    CustomMerger::CustomMergerInitParam getTestParam2()
    {
        CustomMerger::CustomMergerInitParam param; // add index provider
        KeyValueMap parameters;
        //param.resourceProvider = resourceProvider;
        param.parameters = parameters;
        return param;
    }

private:
    config::ResourceReaderPtr _resourceReaderPtr;
    IE_NAMESPACE(misc)::MetricProviderPtr _metricProvider;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergerCreator);

}
}

#endif //ISEARCH_BS_CUSTOMMERGERCREATOR_H
