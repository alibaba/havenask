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
#ifndef ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H
#define ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H

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

class SwiftProcessedDocProducer : public ProcessedDocProducer
{
public:
    SwiftProcessedDocProducer() {};
    ~SwiftProcessedDocProducer() {};

    SwiftProcessedDocProducer(const SwiftProcessedDocProducer&) = delete;
    SwiftProcessedDocProducer& operator=(const SwiftProcessedDocProducer&) = delete;
    SwiftProcessedDocProducer(SwiftProcessedDocProducer&&) = delete;
    SwiftProcessedDocProducer& operator=(SwiftProcessedDocProducer&&) = delete;

public:
    virtual bool init(indexlib::util::MetricProviderPtr metricProvider, const std::string& pluginPath,
                      const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp, int64_t stopTimestamp,
                      int64_t noMoreMsgPeriod, int64_t maxCommitInterval, uint64_t sourceSignature,
                      bool allowSeekCrossSrc = false) = 0;

    virtual void setLinkReporter(const common::SwiftLinkFreshnessReporterPtr& reporter) = 0;

    virtual void resumeRead() = 0;
    virtual void reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg, const std::string& docSource,
                                        bool isReportFastQueueSwiftReadDelay) = 0;
    virtual int64_t getStartTimestamp() const = 0;
    virtual bool needUpdateCommittedCheckpoint() const = 0;
    virtual bool updateCommittedCheckpoint(int64_t checkpoint) = 0;

    virtual bool getMaxTimestamp(int64_t& timestamp) = 0;
    virtual bool getLastReadTimestamp(int64_t& timestamp) = 0;
    virtual uint64_t getLocatorSrc() const = 0;
    virtual void setRecovered(bool isServiceRecovered) = 0;
};

BS_TYPEDEF_PTR(SwiftProcessedDocProducer);

}} // namespace build_service::workflow

#endif // ISEARCH_BS_SWIFTPROCESSEDDOCPRODUCER_H
