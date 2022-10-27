#ifndef ISEARCH_OPTIMIZERCHAINMANAGERCREATOR_H
#define ISEARCH_OPTIMIZERCHAINMANAGERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/OptimizerChainManager.h>
#include <ha3/config/SearchOptimizerConfig.h>
#include <ha3/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(search);

class OptimizerChainManagerCreator
{
public:
    OptimizerChainManagerCreator(const config::ResourceReaderPtr &resourceReaderPtr);
    ~OptimizerChainManagerCreator();
private:
    OptimizerChainManagerCreator(const OptimizerChainManagerCreator &);
    OptimizerChainManagerCreator& operator=(const OptimizerChainManagerCreator &);
private:
    void addBuildInOptimizers(OptimizerChainManagerPtr &optimizerChainManagerPtr);
public:
    OptimizerChainManagerPtr create(const config::SearchOptimizerConfig &optimizerConfig);
    OptimizerChainManagerPtr createFromString(const std::string &configStr);  //for test
private:
    config::ResourceReaderPtr _resourceReaderPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(OptimizerChainManagerCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZERCHAINMANAGERCREATOR_H
