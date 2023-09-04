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

#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace processor {

class BatchRawDocumentDeduper
{
public:
    BatchRawDocumentDeduper(const std::string& dedupField,
                            const indexlib::util::MetricProviderPtr& metricProvider = nullptr);
    ~BatchRawDocumentDeduper();

    BatchRawDocumentDeduper(const BatchRawDocumentDeduper&) = delete;
    BatchRawDocumentDeduper& operator=(const BatchRawDocumentDeduper&) = delete;
    BatchRawDocumentDeduper(BatchRawDocumentDeduper&&) = delete;
    BatchRawDocumentDeduper& operator=(BatchRawDocumentDeduper&&) = delete;

public:
    void process(const document::RawDocumentVecPtr& docQueue);

private:
    std::string _dedupField;
    indexlib::util::MetricPtr _dedupDocQpsMetric;
    indexlib::util::MetricPtr _dedupProcessLatencyMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchRawDocumentDeduper);

}} // namespace build_service::processor
