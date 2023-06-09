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

#include "indexlib/base/Status.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/document/normal/SummaryDocument.h"
#include "indexlib/document/normal/SummaryGroupFormatter.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlib::document {
class SummaryGroupFormatter;
}

namespace indexlibv2 { namespace document {

class SummaryFormatter
{
public:
    SummaryFormatter(const std::shared_ptr<config::SummaryIndexConfig>& summaryConfig);
    SummaryFormatter(const SummaryFormatter& other) = delete;
    ~SummaryFormatter() {}

public:
    // caller: build_service::ClassifiedDocument::serializeSummaryDocument
    Status SerializeSummaryDoc(const std::shared_ptr<indexlib::document::SummaryDocument>& document,
                               std::shared_ptr<indexlib::document::SerializedSummaryDocument>& serDoc);

    void DeserializeSummaryDoc(const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& serDoc,
                               indexlib::document::SearchSummaryDocument* document);

public:
    // public for test
    std::pair<Status, size_t> GetSerializeLength(const std::shared_ptr<indexlib::document::SummaryDocument>& document);

    Status SerializeSummary(const std::shared_ptr<indexlib::document::SummaryDocument>& document, char* value,
                            size_t length) const;

    void TEST_DeserializeSummaryDoc(const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& serDoc,
                                    indexlib::document::SearchSummaryDocument* document);

    bool TEST_DeserializeSummary(indexlib::document::SearchSummaryDocument* document, const char* value,
                                 size_t length) const;

private:
    typedef std::vector<std::shared_ptr<indexlib::document::SummaryGroupFormatter>> GroupFormatterVec;
    typedef std::vector<uint32_t> LengthVec;
    GroupFormatterVec _groupFormatterVec;
    LengthVec _lengthVec;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////
}} // namespace indexlibv2::document
