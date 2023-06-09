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
#include "indexlib/index/operation_log/OperationCreator.h"

namespace indexlibv2::index {
class AttributeConvertor;
class UpdateFieldExtractor;
} // namespace indexlibv2::index
namespace indexlib::index {

class UpdateFieldOperationCreator : public OperationCreator
{
public:
    UpdateFieldOperationCreator(const std::shared_ptr<OperationLogConfig>& opConfig);
    ~UpdateFieldOperationCreator() = default;

    using FieldIdToConvertorMap = std::vector<std::shared_ptr<indexlibv2::index::AttributeConvertor>>;
    using OperationItem = std::pair<fieldid_t, autil::StringView>;

public:
    bool Create(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                OperationBase** operation) override;

private:
    bool CreateOperationItems(autil::mem_pool::Pool* pool, const indexlibv2::document::NormalDocument* doc,
                              OperationItem** items, uint32_t* itemCount);
    void CreateAttrOperationItems(autil::mem_pool::Pool* pool, const std::shared_ptr<document::AttributeDocument>& doc,
                                  indexlibv2::index::UpdateFieldExtractor& fieldExtractor, OperationItem* itemBase);
    void CreateInvertedIndexOperationItems(autil::mem_pool::Pool* pool,
                                           const std::shared_ptr<document::IndexDocument>& doc,
                                           const std::vector<fieldid_t>& updateFieldIds, OperationItem* itemBase);

    autil::StringView DeserializeField(fieldid_t fieldId, const autil::StringView& fieldValue);

    void InitAttributeConvertorMap(const std::shared_ptr<OperationLogConfig>& opConfig);

    bool CreateUpdateFieldOperation(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                                    OperationItem* items, uint32_t itemCount, OperationBase** operation);

private:
    FieldIdToConvertorMap _fieldId2ConvertorMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
