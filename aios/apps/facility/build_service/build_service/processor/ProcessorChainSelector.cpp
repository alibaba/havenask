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
#include "build_service/processor/ProcessorChainSelector.h"

#include "autil/ConstString.h"
#include "autil/StringTokenizer.h"
#include "build_service/config/DocProcessorChainConfig.h"
#include "build_service/config/ProcessorChainSelectorConfig.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

using namespace indexlib::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, ProcessorChainSelector);

char ProcessorChainSelector::SELECT_FIELDS_SEP = 17;
ProcessorChainSelector::ChainIdVector ProcessorChainSelector::EMPTY_CHAIN_IDS;

ProcessorChainSelector::ProcessorChainSelector() {}

ProcessorChainSelector::~ProcessorChainSelector() {}

bool ProcessorChainSelector::init(const DocProcessorChainConfigVecPtr& chainConfigVec,
                                  const ProcessorChainSelectorConfigPtr& chainSelectorConfig)
{
    for (size_t i = 0; i < chainConfigVec->size(); i++) {
        _allChainIds.push_back(i);
    }
    if (!chainSelectorConfig || chainSelectorConfig->selectFields.empty() || chainSelectorConfig->selectRules.empty()) {
        BS_LOG(INFO, "init default ProcessorChainSelector");
        return true;
    }

    map<string, uint16_t> chainNameToChainIdMap;
    for (size_t i = 0; i < chainConfigVec->size(); i++) {
        string chainName = (*chainConfigVec)[i].chainName;
        if (chainName.empty()) {
            BS_LOG(ERROR,
                   "init ProcessorChainSelector failed, "
                   "chainName of chain[%zu] is empty.",
                   i);
            return false;
        }
        if (chainNameToChainIdMap.find(chainName) != chainNameToChainIdMap.end()) {
            BS_LOG(ERROR,
                   "init ProcessorChainSelector failed, "
                   "chainName[%s] is duplicated.",
                   chainName.c_str());
            return false;
        }
        chainNameToChainIdMap.insert(make_pair(chainName, (uint16_t)i));
    }
    _selectFields.insert(_selectFields.end(), chainSelectorConfig->selectFields.begin(),
                         chainSelectorConfig->selectFields.end());

    auto it = chainSelectorConfig->selectRules.begin();
    int32_t idx = 0;
    for (; it != chainSelectorConfig->selectRules.end(); it++) {
        SelectRule rule = (*it);
        ChainIdVector chainIds;
        string chainNames;
        for (auto chainName : rule.destChains) {
            auto chainIdIter = chainNameToChainIdMap.find(chainName);
            if (chainIdIter == chainNameToChainIdMap.end()) {
                BS_LOG(INFO, "dest chain[%s] will not take effect for SelectRule[%d].", chainName.c_str(), idx);
                continue;
            }
            chainIds.push_back(chainIdIter->second);
            chainNames += chainName + " ";
        }
        if (chainIds.empty()) {
            BS_LOG(INFO, "SelectRule[%d] will not take effect.", idx);
            idx++;
            continue;
        }
        string chainKey;
        for (size_t i = 0; i < _selectFields.size(); i++) {
            auto fieldValue = rule.matcher.find(_selectFields[i]);
            if (fieldValue != rule.matcher.end()) {
                chainKey += fieldValue->second;
            }

            if (i != _selectFields.size() - 1) {
                chainKey += SELECT_FIELDS_SEP;
            }
        }
        _chainsMap.insert(make_pair(chainKey, chainIds));
        BS_LOG(INFO, "init chain selector[%s] to chain[%s]", chainKey.c_str(), chainNames.c_str());
        idx++;
    }
    return true;
}

const ProcessorChainSelector::ChainIdVector* ProcessorChainSelector::selectChain(const RawDocumentPtr& rawDoc) const
{
    // when user does not config ProcessorChainSelector
    if (alwaysSelectAllChains() || indexlib::CHECKPOINT_DOC == rawDoc->getDocOperateType()) {
        return &_allChainIds;
    }

    string chainKey;
    for (size_t i = 0; i < _selectFields.size(); i++) {
        string fieldName = _selectFields[i];
        string fieldValue = rawDoc->getField(fieldName);
        if (!fieldValue.empty()) {
            chainKey += fieldValue;
        }
        if (i != _selectFields.size() - 1) {
            chainKey += SELECT_FIELDS_SEP;
        }
    }

    auto chainIds = _chainsMap.find(chainKey);
    if (chainIds == _chainsMap.end()) {
        ERROR_COLLECTOR_LOG(INFO, "cant not find chains for document[%s], fieldValue[%s]", rawDoc->toString().c_str(),
                            chainKey.c_str());
        return NULL;
    }
    return &(chainIds->second);
}

}} // namespace build_service::processor
