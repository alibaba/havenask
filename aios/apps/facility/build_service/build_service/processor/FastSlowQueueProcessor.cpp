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
#include "build_service/processor/FastSlowQueueProcessor.h"

#include <cstdint>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/document/ClassifiedDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Monitor.h"
#include "fslib/fs/File.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"
#include "kmonitor/client/MetricType.h"

using namespace fslib::fs;
using namespace build_service::document;

namespace build_service { namespace processor {

BS_LOG_SETUP(processor, FastSlowQueueProcessor);

FastSlowQueueProcessor::FastSlowQueueProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}
bool FastSlowQueueProcessor::init(const DocProcessorInitParam& param)
{
    std::string configPath = getValueFromKeyValueMap(param.parameters, CONFIG_PATH);
    if (configPath.empty()) {
        BS_LOG(WARN, "%s is empty, fast slow queue disabled", CONFIG_PATH);
        return false;
    }

    std::map<std::string, std::vector<int64_t>> filterMap;
    std::string content;
    if (!param.resourceReader->getConfigContent(configPath, content)) {
        BS_LOG(ERROR, "fast-slow queue config read failed, config path[%s]", configPath.c_str());
        return false;
    }
    try {
        autil::legacy::FromJsonString(filterMap, content);
        if (filterMap.empty()) {
            BS_LOG(ERROR, "invalid FastSlowQueueProcessor [%s], nidVec is empty ", content.c_str());
            return false;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "deserialize FastSlowQueueProcessor [%s] failed, errorMsg [%s]", content.c_str(), e.what());
        return false;
    }
    for (const auto& it : filterMap) {
        if (it.first == NID) {
            _nidFilter.insert(it.second.begin(), it.second.end());
        } else if (it.first == USER_ID) {
            _userIdFilter.insert(it.second.begin(), it.second.end());
        } else {
            BS_LOG(ERROR, "Un-support fast-slow-queue field[%s]", it.first.c_str());
        }
    }

    if (param.metricProvider) {
        _fastDocQpsMetric = DECLARE_METRIC(param.metricProvider, "basic/fastDocQps", kmonitor::QPS, "count");
    }
    BS_LOG(INFO, "init fast-slow queue success");
    return true;
}

bool FastSlowQueueProcessor::isFastQueueDoc(const RawDocumentPtr& rawDocument)
{
    if (!_userIdFilter.empty()) {
        int64_t userId = -1;
        const auto& userIdStr = rawDocument->getField(USER_ID);
        if (!userIdStr.empty()) {
            if (!autil::StringUtil::fromString(userIdStr, userId)) {
                BS_LOG(ERROR, "convert string to int64 failed, userIdStr[%s] userId[%ld]", userIdStr.c_str(), userId);
            }
            if (_userIdFilter.find(userId) != _userIdFilter.end()) {
                return true;
            }
        }
    }
    if (!_nidFilter.empty()) {
        int64_t nid = -1;
        const auto& nidStr = rawDocument->getField(NID);
        if (!nidStr.empty()) {
            if (!autil::StringUtil::fromString(nidStr, nid)) {
                BS_LOG(ERROR, "convert string to int64 failed, nidStr[%s] nid[%ld]", nidStr.c_str(), nid);
            }
            if (_nidFilter.find(nid) != _nidFilter.end()) {
                return true;
            }
        }
    }
    return false;
}

bool FastSlowQueueProcessor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr& processedDoc = document->getProcessedDocument();
    const RawDocumentPtr& rawDocument = document->getRawDocument();
    if (isFastQueueDoc(rawDocument)) {
        BS_LOG(INFO, "nid[%s] user_id[%s] is high-priority doc", rawDocument->getField(NID).c_str(),
               rawDocument->getField(USER_ID).c_str());
        // BS_LOG(INFO, "debug doc[%s] in high doc", rawDocument->toString().c_str());
        ProcessedDocument::DocClusterMetaVec metas = processedDoc->getDocClusterMetaVec();
        for (size_t i = 0; i < metas.size(); i++) {
            // make sure high priority doc enter to fast queue(swift) with the type 255
            metas[i].buildType |= ProcessedDocument::SWIFT_FILTER_BIT_FASTQUEUE;
        }
        processedDoc->setDocClusterMetaVec(metas);
        INCREASE_QPS(_fastDocQpsMetric);
    }
    return true;
}

void FastSlowQueueProcessor::batchProcess(const std::vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

FastSlowQueueProcessor* FastSlowQueueProcessor::clone() { return new FastSlowQueueProcessor(*this); }

void FastSlowQueueProcessor::destroy() { delete this; }

}} // namespace build_service::processor
