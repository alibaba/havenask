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
#include "build_service/processor/TaoliveBusinessProcessor.h"

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, TaoliveBusinessProcessor);

const std::string TaoliveBusinessProcessor::PROCESSOR_NAME("TaoliveBusinessProcessor");

bool TaoliveBusinessProcessor::init(const DocProcessorInitParam& param) { return true; }

DocumentProcessor* TaoliveBusinessProcessor::clone() { return new TaoliveBusinessProcessor(*this); }

void TaoliveBusinessProcessor::destroy() { delete this; }

bool TaoliveBusinessProcessor::process(const document::ExtendDocumentPtr& document)
{
    auto rawDoc = document->getRawDocument();

    // skip events without live_start_time
    const auto& liveStartTime = rawDoc->getField(autil::StringView("live_start_time"));
    if (liveStartTime.empty()) {
        rawDoc->setDocOperateType(SKIP_DOC);
        return true;
    }

    // set live_end_time to max int64_t
    const auto& liveEndTimeStr = rawDoc->getField(autil::StringView("live_end_time"));
    if (liveEndTimeStr.empty()) {
        static const std::string DEFAULT_LIVE_END_TIME = std::to_string(std::numeric_limits<int64_t>::max());
        rawDoc->setField(autil::StringView("live_end_time"), autil::StringView(DEFAULT_LIVE_END_TIME));
    }

    // generate primary key = content_id + visitor_id + reach_time + type + app_key
    const auto& contentId = rawDoc->getField(autil::StringView("content_id"));
    const auto& visitorId = rawDoc->getField(autil::StringView("visitor_id"));
    const auto& reachTime = rawDoc->getField(autil::StringView("reach_time"));
    const auto& type = rawDoc->getField(autil::StringView("type")); // event type
    const auto& appKey = rawDoc->getField(autil::StringView("app_key"));
    size_t size = contentId.size() + visitorId.size() + reachTime.size() + type.size() + appKey.size();
    std::string pk;
    pk.reserve(size);
    pk.append(contentId.data(), contentId.size());
    pk.append(visitorId.data(), visitorId.size());
    pk.append(reachTime.data(), reachTime.size());
    pk.append(type.data(), type.size());
    pk.append(appKey.data(), appKey.size());
    rawDoc->setField(autil::StringView("pk"), autil::StringView(pk));

    return true;
}

void TaoliveBusinessProcessor::batchProcess(const std::vector<document::ExtendDocumentPtr>& docs)
{
    for (const auto& doc : docs) {
        process(doc);
    }
}

}} // namespace build_service::processor
