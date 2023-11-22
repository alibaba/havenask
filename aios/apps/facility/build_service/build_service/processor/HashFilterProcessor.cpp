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
#include "build_service/processor/HashFilterProcessor.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/normal/ClassifiedDocument.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::document;

using namespace indexlib::config;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, HashFilterProcessor);

const string HashFilterProcessor::PROCESSOR_NAME = "HashFilterProcessor";
const string HashFilterProcessor::KEEP_DOC_PERCENT = "keep_doc_percent";

HashFilterProcessor::HashFilterProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _keepDocPercent(100) // default keep all 100% docs
{
}

HashFilterProcessor::~HashFilterProcessor() {}

bool HashFilterProcessor::init(const DocProcessorInitParam& param)
{
    if (autil::EnvUtil::getEnv("DISABLE_HASH_FILETER_PROCESSOR", false)) {
        return true;
    }
    auto iter = param.parameters.find(KEEP_DOC_PERCENT);
    if (iter != param.parameters.end()) {
        if (!autil::StringUtil::strToUInt32(iter->second.c_str(), _keepDocPercent) || _keepDocPercent > 100) {
            BS_LOG(ERROR, "illegal param [%s], expect an interger in range [0, 100], actual [%s]",
                   KEEP_DOC_PERCENT.c_str(), iter->second.c_str());
            return false;
        } else {
            BS_LOG(INFO, "will keep only [%u%%] docs", _keepDocPercent);
        }
    }

    for (const auto& cluster : param.clusterNames) {
        std::string key = string("_") + cluster + "_";
        std::string rangeStr = getValueFromKeyValueMap(param.parameters, key);
        std::vector<std::string> rangeInfo;
        if (!rangeStr.empty()) {
            autil::StringUtil::fromString(rangeStr, rangeInfo, ",");
            if (rangeInfo.size() != 2) {
                BS_LOG(ERROR, "range[%s] invalid for cluster [%s]", rangeStr.c_str(), cluster.c_str());
                return false;
            }
            ClusterRange range;
            range.from = autil::StringUtil::numberFromString<uint64_t>(rangeInfo[0]);
            range.to = autil::StringUtil::numberFromString<uint64_t>(rangeInfo[1]);
            _clusterRanges[cluster] = range;
        }
    }
    return true;
}

void HashFilterProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

bool HashFilterProcessor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr& processedDocPtr = document->getProcessedDocument();
    if (!processedDocPtr) {
        BS_LOG(WARN, "HashFilterProcessor should configured after HashDocumentProcessor");
        return true;
    }
    auto docClusterMetas = processedDocPtr->getDocClusterMetaVec();
    bool hasFilteredCluster = false;
    for (auto& meta : docClusterMetas) {
        if (FilterByHashId(meta.clusterName, meta.hashId)) {
            meta.buildType = ProcessedDocument::SWIFT_FILTER_BIT_ALL;
            hasFilteredCluster = true;
        }
    }
    if (hasFilteredCluster) {
        if (docClusterMetas.size() <= 1u) {
            const RawDocumentPtr& rawDocCumentPtr = document->getRawDocument();
            rawDocCumentPtr->setDocOperateType(SKIP_DOC);
        } else {
            processedDocPtr->setDocClusterMetaVec(docClusterMetas);
        }
    }
    return true;
}

bool HashFilterProcessor::FilterByHashId(const std::string& clusterName, hashid_t hashId) const
{
    if (_keepDocPercent < 100) {
        if (hashId % 101 >= _keepDocPercent) {
            return true;
        }
    }

    auto iter = _clusterRanges.find(clusterName);
    if (iter == _clusterRanges.end()) {
        return false;
    }
    const auto& range = iter->second;
    if (hashId < range.from or hashId > range.to) {
        return true;
    }
    return false;
}

void HashFilterProcessor::destroy() { delete this; }

DocumentProcessor* HashFilterProcessor::clone() { return new HashFilterProcessor(*this); }

}} // namespace build_service::processor
