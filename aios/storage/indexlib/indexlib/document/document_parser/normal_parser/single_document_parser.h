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
#ifndef __INDEXLIB_SINGLE_DOCUMENT_PARSER_H
#define __INDEXLIB_SINGLE_DOCUMENT_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/normal_parser/null_field_appender.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionExtendDocFieldsConvertor);
DECLARE_REFERENCE_CLASS(document, SectionAttributeAppender);
DECLARE_REFERENCE_CLASS(document, MultiRegionPackAttributeAppender);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

namespace indexlib { namespace document {

class SingleDocumentParser
{
public:
    SingleDocumentParser();
    ~SingleDocumentParser();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, util::AccumulativeCounterPtr& attrConvertErrorCounter);
    NormalDocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc);

private:
    void SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document, const config::IndexSchemaPtr& indexSchema,
                            regionid_t regionId);

    void AddModifiedFields(const IndexlibExtendDocumentPtr& document, const NormalDocumentPtr& indexDoc);

    NormalDocumentPtr CreateDocument(const IndexlibExtendDocumentPtr& document);

    bool Validate(const IndexlibExtendDocumentPtr& document);

private:
    config::IndexPartitionSchemaPtr mSchema;
    MultiRegionExtendDocFieldsConvertorPtr mFieldConvertPtr;
    SectionAttributeAppenderPtr mSectionAttrAppender;
    MultiRegionPackAttributeAppenderPtr mPackAttrAppender;
    NullFieldAppenderPtr mNullFieldAppender;
    bool mHasPrimaryKey;
    indexlib::util::AccumulativeCounterPtr mAttributeConvertErrorCounter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleDocumentParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_SINGLE_DOCUMENT_PARSER_H
