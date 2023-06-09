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
#ifndef __INDEXLIB_SUMMARY_FORMATTER_H
#define __INDEXLIB_SUMMARY_FORMATTER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/summary_schema.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class SummaryFormatter
{
public:
    SummaryFormatter(const config::SummarySchemaPtr& summarySchema);
    SummaryFormatter(const SummaryFormatter& other) = delete;
    ~SummaryFormatter() {}

public:
    // caller: build_service::ClassifiedDocument::serializeSummaryDocument
    void SerializeSummaryDoc(const document::SummaryDocumentPtr& document,
                             document::SerializedSummaryDocumentPtr& serDoc);

    void DeserializeSummaryDoc(const document::SerializedSummaryDocumentPtr& serDoc,
                               document::SearchSummaryDocument* document);

public:
    // public for test
    size_t GetSerializeLength(const document::SummaryDocumentPtr& document);

    void SerializeSummary(const document::SummaryDocumentPtr& document, char* value, size_t length) const;

    void TEST_DeserializeSummaryDoc(const document::SerializedSummaryDocumentPtr& serDoc,
                                    document::SearchSummaryDocument* document);

    bool TEST_DeserializeSummary(document::SearchSummaryDocument* document, const char* value, size_t length) const;

private:
    typedef std::vector<SummaryGroupFormatterPtr> GroupFormatterVec;
    typedef std::vector<uint32_t> LengthVec;
    GroupFormatterVec mGroupFormatterVec;
    LengthVec mLengthVec;
    config::SummarySchemaPtr mSummarySchema;

private:
    friend class SummaryFormatterTest;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryFormatter> SummaryFormatterPtr;

///////////////////////////////////////////////////////////////////////////////
}} // namespace indexlib::document

#endif //__INDEXLIB_SUMMARY_FORMATTER_H
