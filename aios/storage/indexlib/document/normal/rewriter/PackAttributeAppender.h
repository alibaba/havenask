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

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlib::document {
class AttributeDocument;
}

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2 { namespace document {

class PackAttributeAppender
{
public:
    typedef std::vector<std::shared_ptr<index::PackAttributeFormatter>> FormatterVector;

public:
    PackAttributeAppender() {}
    ~PackAttributeAppender() {}

public:
    std::pair<Status, bool> Init(const std::shared_ptr<config::ITabletSchema>& schema);

    bool AppendPackAttribute(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDocument,
                             autil::mem_pool::Pool* pool);

    bool EncodePatchValues(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDocument,
                           autil::mem_pool::Pool* pool);

    bool DecodePatchValues(uint8_t* buffer, size_t bufferLen, packattrid_t packId,
                           index::PackAttributeFormatter::PackAttributeFields& patchFields);

    autil::StringView
    MergeAndFormatUpdateFields(const char* baseAddr,
                               const index::PackAttributeFormatter::PackAttributeFields& packAttrFields,
                               bool hasHashKeyInAttrFields, indexlib::util::MemBuffer& buffer, packattrid_t packId);

private:
    bool InitOnePackAttr(const std::shared_ptr<index::PackAttributeConfig>& packAttrConfig);
    bool CheckPackAttrFields(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDocument);

private:
    FormatterVector _packFormatters;
    // std::vector<fieldid_t> _inPackFields;
    std::vector<fieldid_t> _clearFields;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
