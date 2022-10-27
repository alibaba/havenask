#include "build_service/config/ProcessorChainSelectorConfig.h"

using namespace std;

namespace build_service {
namespace config {

SelectRule::SelectRule()
{}

SelectRule::~SelectRule()
{}

bool SelectRule::operator == (const SelectRule &other) const
{
    return matcher == other.matcher && destChains == other.destChains;
}

bool SelectRule::operator != (const SelectRule &other) const
{
    return !(SelectRule::operator==(other));
}

void SelectRule::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("matcher", matcher, matcher);
    json.Jsonize("dest_chains", destChains, destChains);
}


ProcessorChainSelectorConfig::ProcessorChainSelectorConfig()
{
}

ProcessorChainSelectorConfig::~ProcessorChainSelectorConfig()
{
}

void ProcessorChainSelectorConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
{
    json.Jsonize("select_fields", selectFields, selectFields);
    json.Jsonize("select_rules", selectRules, selectRules);
}
    
}
}
