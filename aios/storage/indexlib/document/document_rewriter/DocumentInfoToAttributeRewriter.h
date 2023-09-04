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
#include "autil/Log.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/document_rewriter/IDocumentRewriter.h"

namespace indexlibv2::index {
class AttributeConvertor;
class AttributeConfig;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class FieldConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {

class DocumentInfoToAttributeRewriter : public IDocumentRewriter
{
public:
    DocumentInfoToAttributeRewriter(const std::shared_ptr<index::AttributeConfig>& timestampAttrConfig,
                                    const std::shared_ptr<index::AttributeConfig>& hashIdAttrConfig,
                                    const std::shared_ptr<index::AttributeConfig>& concurrentIdxAttrConfig);
    ~DocumentInfoToAttributeRewriter();
    static inline const std::string VIRTUAL_HASH_ID_FIELD_NAME = "virtual_hash_id_field_name";
    static constexpr FieldType VIRTUAL_HASH_ID_FIELD_TYPE = ft_uint16;
    static inline const std::string VIRTUAL_TIMESTAMP_FIELD_NAME = "virtual_timestamp_field_name";
    static constexpr FieldType VIRTUAL_TIMESTAMP_FIELD_TYPE = ft_int64;
    static inline const std::string VIRTUAL_CONCURRENT_IDX_FIELD_NAME = "virtual_concurrent_idx_name";
    static constexpr FieldType VIRTUAL_CONCURRENT_IDX_FIELD_TYPE = ft_uint32;

public:
    Status Rewrite(document::IDocumentBatch* batch) override;

private:
    std::shared_ptr<index::AttributeConvertor> _timestampConvertor;
    std::shared_ptr<index::AttributeConvertor> _hashIdConvertor;
    std::shared_ptr<index::AttributeConvertor> _concurrentIdxConvertor;
    fieldid_t _timestampFieldId;
    fieldid_t _hashIdFieldId;
    fieldid_t _concurrentFieldId;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
