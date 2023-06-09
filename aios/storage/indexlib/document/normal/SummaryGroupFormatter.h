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

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/summary/Types.h"

namespace indexlib::util {
class BufferCompressor;
}

namespace indexlibv2::config {
class SummaryGroupConfig;
}

namespace indexlib { namespace document {
class SummaryDocument;
class SearchSummaryDocument;
class GroupFieldFormatter;

class SummaryGroupFormatter
{
public:
    SummaryGroupFormatter(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig);
    SummaryGroupFormatter(const SummaryGroupFormatter& other);
    ~SummaryGroupFormatter();

public:
    std::pair<Status, size_t> GetSerializeLength(const std::shared_ptr<SummaryDocument>& document) const;
    Status SerializeSummary(const std::shared_ptr<SummaryDocument>& document, char* value, size_t length) const;
    bool DeserializeSummary(document::SearchSummaryDocument* document, const char* value, size_t length) const;

private:
    size_t GetSerializeLengthWithCompress(const std::shared_ptr<SummaryDocument>& document) const;
    bool DoDeserializeSummary(document::SearchSummaryDocument* document, const char* value, size_t length) const;

private:
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> _summaryGroupConfig;
    index::summaryfieldid_t _summaryFieldIdBase;
    std::shared_ptr<GroupFieldFormatter> _groupFieldFormatter;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
