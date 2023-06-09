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
#ifndef __INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, PackageIndexConfig);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);
DECLARE_REFERENCE_CLASS(common, SectionAttributeFormatter);
DECLARE_REFERENCE_CLASS(common, StringAttributeConvertor);

namespace indexlib { namespace document {

/**
 * notice: this Class is not thread-safe.
 */
class SectionAttributeAppender
{
public:
    struct IndexMeta {
        config::PackageIndexConfigPtr packIndexConfig;
        common::SectionAttributeFormatterPtr formatter;
        common::StringAttributeConvertorPtr convertor;
    };
    typedef std::vector<IndexMeta> IndexMetaVec;

public:
    SectionAttributeAppender();
    SectionAttributeAppender(const SectionAttributeAppender& other);
    ~SectionAttributeAppender();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);
    bool AppendSectionAttribute(const document::IndexDocumentPtr& indexDocument);
    SectionAttributeAppender* Clone();

private:
    void AppendSectionAttributeForOneIndex(const IndexMeta& indexMeta, const document::IndexDocumentPtr& indexDocument);

    void AppendSectionAttributeForOneField(document::IndexTokenizeField* field, int32_t dGapFid, bool hasFid,
                                           bool hasSectionWeight);

    void EncodeSectionAttributeForOneIndex(const IndexMeta& indexMeta, const document::IndexDocumentPtr& indexDocument);

private:
    IndexMetaVec mIndexMetaVec;
    section_fid_t mSectionFids[MAX_SECTION_COUNT_PER_DOC];
    section_len_t mSectionLens[MAX_SECTION_COUNT_PER_DOC];
    section_weight_t mSectionWeights[MAX_SECTION_COUNT_PER_DOC];
    uint32_t mSectionsCountInCurrentDoc;
    uint32_t mTotalSectionLen;
    bool mSectionOverFlowFlag;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeAppender);
}} // namespace indexlib::document

#endif //__INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H
