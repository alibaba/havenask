#ifndef ISEARCH_OPTIMIZERCONFIGINFO_H
#define ISEARCH_OPTIMIZERCONFIGINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

struct OptimizerConfigInfo : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        KeyValueMap defaultParams;
        json.Jsonize("optimizer_name", optimizerName, optimizerName);
        json.Jsonize("module_name", moduleName, moduleName);
        json.Jsonize("parameters", parameters, defaultParams);
    }
    std::string optimizerName;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<OptimizerConfigInfo> OptimizerConfigInfos;

END_HA3_NAMESPACE(config);

#endif //ISEARCH_OPTIMIZERCONFIGINFO_H
