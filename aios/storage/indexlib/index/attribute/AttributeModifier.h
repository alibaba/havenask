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
#include "autil/NoCopyable.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/ITabletSchema.h"

namespace indexlibv2 { namespace framework {
class TabletData;
}} // namespace indexlibv2::framework

namespace indexlibv2::index {
class AttributeModifier : public autil::NoCopyable
{
public:
    AttributeModifier(const std::shared_ptr<config::ITabletSchema>& schema) : _schema(schema) {}

    virtual ~AttributeModifier() = default;

public:
    virtual Status Init(const framework::TabletData& tabletData) = 0;
    virtual bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) = 0;

public:
    bool UpdateFieldWithName(docid_t docId, const std::string& fieldName, const autil::StringView& value, bool isNull)
    {
        auto fieldId = _schema->GetFieldConfig(fieldName)->GetFieldId();
        return UpdateField(docId, fieldId, value, isNull);
    }

protected:
    std::shared_ptr<config::ITabletSchema> _schema;
};

} // namespace indexlibv2::index
