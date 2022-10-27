#ifndef ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H
#define ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {
class SelectRule : public autil::legacy::Jsonizable
{
public:
    SelectRule();
    ~SelectRule();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator == (const SelectRule &other) const;
    bool operator != (const SelectRule &other) const;
public:
    std::map<std::string, std::string> matcher;
    std::vector<std::string> destChains;
};
typedef std::vector<SelectRule> SelectRuleVec;

class ProcessorChainSelectorConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorChainSelectorConfig();
    ~ProcessorChainSelectorConfig();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

public:
    std::vector<std::string> selectFields;
    SelectRuleVec selectRules;
};

BS_TYPEDEF_PTR(ProcessorChainSelectorConfig);
typedef std::vector<ProcessorChainSelectorConfig> ProcessorChainSelectorConfigVec;

}
}

#endif //ISEARCH_BS_PROCESSORCHAINSELECTORCONFIG_H
