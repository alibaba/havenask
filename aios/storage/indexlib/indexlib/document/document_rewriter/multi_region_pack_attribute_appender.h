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

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class MultiRegionPackAttributeAppender
{
public:
    MultiRegionPackAttributeAppender();
    ~MultiRegionPackAttributeAppender();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);

    bool AppendPackAttribute(const document::AttributeDocumentPtr& attrDocument, autil::mem_pool::Pool* pool,
                             regionid_t regionId);

    bool EncodePatchValues(const document::AttributeDocumentPtr& attrDocument, autil::mem_pool::Pool* pool,
                           regionid_t regionId);

    bool DecodePatchValues(uint8_t* buffer, size_t bufferLen, regionid_t regionId, packattrid_t packId,
                           common::PackAttributeFormatter::PackAttributeFields& patchFields);

    autil::StringView MergeAndFormatUpdateFields(
        const char* baseAddr, const common::PackAttributeFormatter::PackAttributeFields& packAttrFields,
        bool hasHashKeyInAttrFields, util::MemBuffer& buffer, regionid_t regionId, packattrid_t packId);

private:
    std::vector<PackAttributeAppenderPtr> mAppenders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionPackAttributeAppender);
}} // namespace indexlib::document
