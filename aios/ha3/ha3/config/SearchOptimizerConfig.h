#ifndef ISEARCH_SEARCHOPTIMIZERCONFIG_H
#define ISEARCH_SEARCHOPTIMIZERCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/OptimizerConfigInfo.h>
#include <build_service/plugin/ModuleInfo.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class SearchOptimizerConfig : public autil::legacy::Jsonizable
{
public:
    SearchOptimizerConfig();
    ~SearchOptimizerConfig();
private:
    SearchOptimizerConfig(const SearchOptimizerConfig &);
    SearchOptimizerConfig& operator=(const SearchOptimizerConfig &);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    const build_service::plugin::ModuleInfos& getModuleInfos() const {
        return _modules;
    }
    const OptimizerConfigInfos& getOptimizerConfigInfos() const {
        return _optimizerConfigInfos;
    }
private:
    build_service::plugin::ModuleInfos _modules;
    OptimizerConfigInfos _optimizerConfigInfos;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearchOptimizerConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SEARCHOPTIMIZERCONFIG_H
