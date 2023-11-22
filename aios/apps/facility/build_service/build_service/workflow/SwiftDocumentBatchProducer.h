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
#pragma once

#include "build_service/workflow/Producer.h"
#include "swift/client/SwiftReader.h"

namespace indexlib { namespace util {
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util
namespace indexlib { namespace util {
class CounterMap;
typedef std::shared_ptr<CounterMap> CounterMapPtr;
}} // namespace indexlib::util
BS_DECLARE_REFERENCE_CLASS(common, SwiftLinkFreshnessReporter);

namespace build_service { namespace workflow {

class SwiftDocumentBatchProducer : public DocumentBatchProducer
{
public:
    SwiftDocumentBatchProducer() {};
    ~SwiftDocumentBatchProducer() {};

    SwiftDocumentBatchProducer(const SwiftDocumentBatchProducer&) = delete;
    SwiftDocumentBatchProducer& operator=(const SwiftDocumentBatchProducer&) = delete;
    SwiftDocumentBatchProducer(SwiftDocumentBatchProducer&&) = delete;
    SwiftDocumentBatchProducer& operator=(SwiftDocumentBatchProducer&&) = delete;

public:
    virtual void reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg) = 0;
    virtual bool getMaxTimestamp(int64_t& timestamp) const = 0;
    virtual bool getLastReadTimestamp(int64_t& timestamp) const = 0;
    virtual void setRecovered(bool isServiceRecovered) = 0;
};

BS_TYPEDEF_PTR(SwiftDocumentBatchProducer);

}} // namespace build_service::workflow
