#pragma once

#include <ha3/common.h>
#include <suez/turing/expression/plugin/CavaScorerAdapter.h>

BEGIN_HA3_NAMESPACE(rank);

class CavaScorerAdapter : public suez::turing::CavaScorerAdapter
{
public:
    CavaScorerAdapter(const suez::turing::KeyValueMap &scorerParameters,
                      const suez::turing::CavaPluginManagerPtr &cavaPluginManager,
                      kmonitor::MetricsReporter *metricsReporter);
    suez::turing::Scorer *clone() override;
private:
    bool callBeginRequestFunc(suez::turing::ScoringProvider *provider) override;
protected:
    std::string getConfigValue(suez::turing::ScoringProvider *provider,
                               const std::string &configKey) override;
};

HA3_TYPEDEF_PTR(CavaScorerAdapter);
END_HA3_NAMESPACE(rank);
