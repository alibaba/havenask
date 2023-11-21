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
#include "indexlib/document/document_parser.h"
#include "indexlib/document/document_parser/normal_parser/single_document_parser.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/source_timestamp_parser.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace document {

// TODO: add test case
class NormalDocumentParser : public DocumentParser
{
public:
    NormalDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~NormalDocumentParser();

public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::StringView& serializedData) override;

private:
    bool IsEmptyUpdate(const NormalDocumentPtr& doc);
    bool InitFromDocumentParam();
    void ReportModifyFieldQps(const NormalDocumentPtr& doc);
    void ReportIndexAddToUpdateQps(const NormalDocument& doc);
    void ReportAddToUpdateFailQps(const NormalDocument& doc);

private:
    SingleDocumentParserPtr mMainParser;
    SingleDocumentParserPtr mSubParser;
    std::vector<DocumentRewriterPtr> mDocRewriters;
    IE_DECLARE_METRIC(UselessUpdateQps);
    IE_DECLARE_METRIC(IndexUselessUpdateQps);
    IE_DECLARE_METRIC(ModifyFieldQps);
    IE_DECLARE_METRIC(IndexUpdateQps);
    IE_DECLARE_METRIC(AttributeUpdateQps);
    IE_DECLARE_METRIC(SummaryUpdateQps);
    IE_DECLARE_METRIC(SingleModifyFieldQps);
    IE_DECLARE_METRIC(DocParserQps);
    IE_DECLARE_METRIC(IndexAddToUpdateQps);
    IE_DECLARE_METRIC(AddToUpdateFailedQps);
    indexlib::util::AccumulativeCounterPtr mAttributeConvertErrorCounter;
    bool mSupportNull;
    bool mEnableModifyFieldStat;
    std::unique_ptr<SourceTimestampParser> mSourceTimestampParser = nullptr;

public:
    static std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalDocumentParser);
}} // namespace indexlib::document
