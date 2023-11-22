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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(config, SummarySchema);
DECLARE_REFERENCE_CLASS(document, SerializedSummaryDocument);
DECLARE_REFERENCE_CLASS(index, SummarySegmentReader);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);

namespace indexlib { namespace index {

#define DECLARE_SUMMARY_WRITER_IDENTIFIER(id)                                                                          \
    static std::string Identifier() { return std::string("summary.writer." #id); }                                     \
    virtual std::string GetIdentifier() const { return Identifier(); }

class SummaryWriter
{
public:
    SummaryWriter() {}
    virtual ~SummaryWriter() {}

public:
    virtual void Init(const config::SummarySchemaPtr& summarySchema,
                      util::BuildResourceMetrics* buildResourceMetrics) = 0;
    virtual void AddDocument(const document::SerializedSummaryDocumentPtr& document) = 0;
    // virtual void Dump(const std::string& dir) = 0;
    virtual void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) = 0;
    virtual const SummarySegmentReaderPtr CreateInMemSegmentReader() = 0;

    void SetBuildProfilingMetrics(const index::BuildProfilingMetricsPtr& metrics) { mProfilingMetrics = metrics; }
    void SetTemperatureLayer(const std::string& temperature) { mTemperatureLayer = temperature; }

protected:
    index::BuildProfilingMetricsPtr mProfilingMetrics;
    std::string mTemperatureLayer;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryWriter> SummaryWriterPtr;
}} // namespace indexlib::index
