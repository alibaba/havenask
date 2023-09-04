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
#include <stdint.h>
#include <string>

namespace indexlibv2::document::extractor {

enum class DocumentInfoExtractorType {
    PRIMARY_KEY,
    ATTRIBUTE_DOC,
    ATTRIBUTE_FIELD,
    PACK_ATTRIBUTE_FIELD,
    INVERTED_INDEX_DOC,
    SUMMARY_DOC,
    SERIES_DOC,
    UNKNOWN,
};

inline std::string DocumentInfoExtractorTypeToStr(DocumentInfoExtractorType docInfoExtractorType)
{
    switch (docInfoExtractorType) {
    case DocumentInfoExtractorType::PRIMARY_KEY:
        return "PRIMARY_KEY";
    case DocumentInfoExtractorType::ATTRIBUTE_DOC:
        return "ATTRIBUTE_DOC";
    case DocumentInfoExtractorType::ATTRIBUTE_FIELD:
        return "ATTRIBUTE_FIELD";
    case DocumentInfoExtractorType::PACK_ATTRIBUTE_FIELD:
        return "PACK_ATTRIBUTE_FIELD";
    case DocumentInfoExtractorType::INVERTED_INDEX_DOC:
        return "INVERTED_INDEX_DOC";
    case DocumentInfoExtractorType::SUMMARY_DOC:
        return "SUMMARY_DOC";
    case DocumentInfoExtractorType::SERIES_DOC:
        return "SERIES_DOC";
    case DocumentInfoExtractorType::UNKNOWN:
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

} // namespace indexlibv2::document::extractor
