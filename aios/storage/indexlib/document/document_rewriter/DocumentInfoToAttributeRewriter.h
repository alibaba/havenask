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
#include "indexlib/document/IDocument.h"
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
                                    const std::shared_ptr<index::AttributeConfig>& docInfoAttrConfig);
    ~DocumentInfoToAttributeRewriter();
    static inline const uint8_t DOC_INFO_SERIALIZE_VERSION = 0;
    static inline const std::string VIRTUAL_TIMESTAMP_FIELD_NAME = "virtual_timestamp_field_name";
    static constexpr FieldType VIRTUAL_TIMESTAMP_FIELD_TYPE = ft_int64;
    // doc info索引存储部分信息
    // [----version----][----source idx----][----hash id----][----concurrent idx----]
    // [----1  byte----][------1 byte------][-----2 byte----][--------4 byte--------]
    static inline const std::string VIRTUAL_DOC_INFO_FIELD_NAME = "virtual_doc_info_name";
    static constexpr FieldType VIRTUAL_DOC_INFO_FIELD_TYPE = ft_uint64;

public:
    Status Rewrite(document::IDocumentBatch* batch) override;
    static uint64_t EncodeDocInfo(const framework::Locator::DocInfo& docInfo);
    static std::optional<framework::Locator::DocInfo> DecodeDocInfo(uint64_t docInfoValue, int64_t timestamp);

private:
    std::shared_ptr<index::AttributeConvertor> _timestampConvertor;
    std::shared_ptr<index::AttributeConvertor> _docInfoConvertor;
    fieldid_t _timestampFieldId;
    fieldid_t _docInfoFieldId;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
