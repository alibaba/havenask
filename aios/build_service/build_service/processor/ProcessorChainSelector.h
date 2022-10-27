#ifndef ISEARCH_BS_PROCESSORCHAINSELECTOR_H
#define ISEARCH_BS_PROCESSORCHAINSELECTOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/document/RawDocument.h"

namespace build_service {
namespace processor {

class ProcessorChainSelector
{
public:
    typedef std::vector<uint16_t> ChainIdVector;
    
public:
    ProcessorChainSelector();
    ~ProcessorChainSelector();

public:
    bool init(const config::DocProcessorChainConfigVecPtr &chainConfigVec,
              const config::ProcessorChainSelectorConfigPtr &chainSelectorConfig);
    
    const ChainIdVector* selectChain(const document::RawDocumentPtr &rawDoc) const;

    bool alwaysSelectAllChains() const
    { return _selectFields.empty() || _chainsMap.empty(); }
    
private:
    std::vector<std::string> _selectFields;
    std::map<std::string, ChainIdVector> _chainsMap;
    ChainIdVector _allChainIds;
private:
    static ChainIdVector EMPTY_CHAIN_IDS;
    static char SELECT_FIELDS_SEP;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorChainSelector);

}
}

#endif //ISEARCH_BS_PROCESSORCHAINSELECTOR_H
