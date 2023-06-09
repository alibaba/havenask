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
#ifndef __INDEXLIB_EXTENDDOCFIELD2INDEXDOCFIELD_H
#define __INDEXLIB_EXTENDDOCFIELD2INDEXDOCFIELD_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_field_convertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"

namespace indexlib { namespace test {

class ExtendDocField2IndexDocField
{
public:
    ExtendDocField2IndexDocField(const config::IndexPartitionSchemaPtr& schema, regionid_t regionId = DEFAULT_REGIONID);
    ~ExtendDocField2IndexDocField();

private:
    ExtendDocField2IndexDocField(const ExtendDocField2IndexDocField&);
    ExtendDocField2IndexDocField& operator=(const ExtendDocField2IndexDocField&);

public:
    void convertAttributeField(const document::NormalDocumentPtr& doc, const config::FieldConfigPtr& fieldConfig,
                               const RawDocumentPtr& rawDoc);
    void convertIndexField(const document::NormalDocumentPtr& doc, const config::FieldConfigPtr& fieldConfig,
                           const RawDocumentPtr& rawDoc);
    void convertSummaryField(const document::SummaryDocumentPtr& doc, const config::FieldConfigPtr& fieldConfig,
                             const RawDocumentPtr& rawDoc);
    void convertModifyFields(const document::NormalDocumentPtr& doc, const std::string& modifyFields);

    void convertIndexField(const document::IndexDocumentPtr& indexDoc, const config::FieldConfigPtr& fieldConfig,
                           const std::string& fieldStr, const std::string& fieldSep, autil::mem_pool::Pool* pool);

private:
    void init();
    void initAttrConvert();

private:
    using AttributeConvertorPtr = std::shared_ptr<indexlibv2::index::AttributeConvertor>;
    using AttributeConvertorVector = std::vector<AttributeConvertorPtr>;

private:
    AttributeConvertorVector mAttrConvertVec;
    config::IndexPartitionSchemaPtr mSchema;
    regionid_t mRegionId;
    document::IndexFieldConvertor mIndexFieldConvertor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExtendDocField2IndexDocField);
}} // namespace indexlib::test

#endif //__INDEXLIB_EXTENDDOCFIELD2INDEXDOCFIELD_H
