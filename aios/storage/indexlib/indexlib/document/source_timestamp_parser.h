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

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"

namespace indexlib { namespace document {

class SourceTimestampParser
{
public:
    SourceTimestampParser(config::IndexPartitionSchemaPtr schema) : _schema(schema) {}
    ~SourceTimestampParser() = default;

public:
    void Init();
    void Parse(const IndexlibExtendDocumentPtr& extendDoc, Document* docment);

private:
    void ExtractSouceTimestampFields(regionid_t regionId);

public:
    static constexpr const char* SOURCE_TIMESTAMP_REPORT_KEY = "source_timestamp_report_key";
    static constexpr const char* SOURCE_TIMESTAMP_UNIT_KEY = "source_timestamp_unit";
    static constexpr const char* SOURCE_TIMESTAMP_UNIT_SECOND = "sec";
    static constexpr const char* SOURCE_TIMESTAMP_UNIT_MILLISECOND = "ms";
    static constexpr const char* SOURCE_TIMESTAMP_UNIT_MICROSECOND = "us";
    static constexpr const char* SOURCE_TIMESTAMP_TAG_NAME = "_source_timestamp_tag_";
    static constexpr const char* SOURCE_TIMESTAMP_INNER_SEPARATOR = "|";
    static constexpr const char* SOURCE_TIMESTAMP_OUTER_SEPARATOR = ";";
    static constexpr int32_t MAX_SOURCE_COUNT = 20;

private:
    typedef std::pair<std::string, std::string> RawFieldToSource;
    std::unordered_map<regionid_t, std::vector<RawFieldToSource>> _regionIdToPairs;
    config::IndexPartitionSchemaPtr _schema;
    int32_t _reportSourceCount = MAX_SOURCE_COUNT;
    IE_LOG_DECLARE();
};

}} // namespace indexlib::document
