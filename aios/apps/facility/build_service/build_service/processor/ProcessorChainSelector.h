/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ISEARCH_BS_PROCESSORCHAINSELECTOR_H
#define ISEARCH_BS_PROCESSORCHAINSELECTOR_H

#include "build_service/common_define.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class ProcessorChainSelector
{
public:
    typedef std::vector<uint16_t> ChainIdVector;

public:
    ProcessorChainSelector();
    ~ProcessorChainSelector();

public:
    bool init(const config::DocProcessorChainConfigVecPtr& chainConfigVec,
              const config::ProcessorChainSelectorConfigPtr& chainSelectorConfig);

    const ChainIdVector* selectChain(const document::RawDocumentPtr& rawDoc) const;

    bool alwaysSelectAllChains() const { return _selectFields.empty() || _chainsMap.empty(); }

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

}} // namespace build_service::processor

#endif // ISEARCH_BS_PROCESSORCHAINSELECTOR_H
