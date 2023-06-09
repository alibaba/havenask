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
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlibv2::config {
class PackageIndexConfig;
class TabletSchema;
} // namespace indexlibv2::config

namespace indexlib::document {
class IndexDocument;
class IndexTokenizeField;
} // namespace indexlib::document

namespace indexlib::index {
class SectionAttributeFormatter;
} // namespace indexlib::index

namespace indexlibv2::index {
class StringAttributeConvertor;
} // namespace indexlibv2::index

namespace indexlibv2 { namespace document {

/**
 * notice: this Class is not thread-safe.
 */
class SectionAttributeAppender
{
public:
    struct IndexMeta {
        std::shared_ptr<config::PackageIndexConfig> packIndexConfig;
        std::shared_ptr<indexlib::index::SectionAttributeFormatter> formatter;
        std::shared_ptr<index::StringAttributeConvertor> convertor;
    };
    typedef std::vector<IndexMeta> IndexMetaVec;

public:
    SectionAttributeAppender();
    SectionAttributeAppender(const SectionAttributeAppender& other);
    ~SectionAttributeAppender();

public:
    bool Init(const std::shared_ptr<config::TabletSchema>& schema);
    std::pair<Status, bool>
    AppendSectionAttribute(const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument);
    SectionAttributeAppender* Clone();

private:
    Status AppendSectionAttributeForOneIndex(const IndexMeta& indexMeta,
                                             const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument);

    void AppendSectionAttributeForOneField(indexlib::document::IndexTokenizeField* field, int32_t dGapFid, bool hasFid,
                                           bool hasSectionWeight);

    Status EncodeSectionAttributeForOneIndex(const IndexMeta& indexMeta,
                                             const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument);

private:
    IndexMetaVec _indexMetaVec;
    section_fid_t _sectionFids[MAX_SECTION_COUNT_PER_DOC];
    section_len_t _sectionLens[MAX_SECTION_COUNT_PER_DOC];
    section_weight_t _sectionWeights[MAX_SECTION_COUNT_PER_DOC];
    uint32_t _sectionsCountInCurrentDoc = 0;
    uint32_t _totalSectionLen = 0;
    bool _sectionOverFlowFlag = false;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
