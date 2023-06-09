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

#include <map>
#include <memory>
#include <mutex>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/util/Singleton.h"

namespace indexlib::config {
class FieldConfig;
}

namespace indexlibv2::index {

template <class INSTANCE, class CREATOR>
class AttributeFactory : public indexlib::util::Singleton<AttributeFactory<INSTANCE, CREATOR>>
{
public:
    using CreatorMap = std::map<FieldType, std::unique_ptr<CREATOR>>;

protected:
    AttributeFactory();
    friend class indexlib::util::LazyInstantiation;

public:
    ~AttributeFactory() = default;

public:
    void RegisterSingleValueCreator(std::unique_ptr<CREATOR> creator);
    void RegisterMultiValueCreator(std::unique_ptr<CREATOR> creator);

    CREATOR* GetAttributeInstanceCreator(const std::shared_ptr<config::IIndexConfig>& indexConfig) const;

private:
    void Init();

private:
    // TODO(xiuchen & yanke) need recursive ?
    mutable std::mutex _mutex;
    CreatorMap _singleValueCreators;
    CreatorMap _multiValueCreators;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, AttributeFactory, T1, T2);

template <class INSTANCE, class CREATOR>
AttributeFactory<INSTANCE, CREATOR>::AttributeFactory()
{
    Init();
}

template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueInt8(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueUInt8(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueInt16(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueUInt16(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueInt32(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueUInt32(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueInt64(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueUInt64(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueFloat(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueDouble(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterMultiValueMultiString(AttributeFactory<INSTANCE, CREATOR>* factory);

template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueInt8(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueUInt8(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueInt16(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueUInt16(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueInt32(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueUInt32(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueInt64(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueUInt64(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueFloat(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueDouble(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSingleValueUInt128(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>

extern void RegisterSpecialMultiValueLine(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSpecialMultiValuePolygon(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSpecialMultiValueLocation(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>

extern void RegisterSpecialSingleValueTime(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSpecialSingleValueTimestamp(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterSpecialSingleValueDate(AttributeFactory<INSTANCE, CREATOR>* factory);
template <class INSTANCE, class CREATOR>
extern void RegisterAttributeString(AttributeFactory<INSTANCE, CREATOR>* factory);

template <class INSTANCE, class CREATOR>
void AttributeFactory<INSTANCE, CREATOR>::Init()
{
    RegisterMultiValueInt8<INSTANCE, CREATOR>(this);
    RegisterMultiValueUInt8<INSTANCE, CREATOR>(this);
    RegisterMultiValueInt16<INSTANCE, CREATOR>(this);
    RegisterMultiValueUInt16<INSTANCE, CREATOR>(this);
    RegisterMultiValueInt32<INSTANCE, CREATOR>(this);
    RegisterMultiValueUInt32<INSTANCE, CREATOR>(this);
    RegisterMultiValueInt64<INSTANCE, CREATOR>(this);
    RegisterMultiValueUInt64<INSTANCE, CREATOR>(this);
    RegisterMultiValueFloat<INSTANCE, CREATOR>(this);
    RegisterMultiValueDouble<INSTANCE, CREATOR>(this);
    RegisterMultiValueMultiString<INSTANCE, CREATOR>(this);

    RegisterSingleValueInt8<INSTANCE, CREATOR>(this);
    RegisterSingleValueUInt8<INSTANCE, CREATOR>(this);
    RegisterSingleValueInt16<INSTANCE, CREATOR>(this);
    RegisterSingleValueUInt16<INSTANCE, CREATOR>(this);
    RegisterSingleValueInt32<INSTANCE, CREATOR>(this);
    RegisterSingleValueUInt32<INSTANCE, CREATOR>(this);
    RegisterSingleValueInt64<INSTANCE, CREATOR>(this);
    RegisterSingleValueUInt64<INSTANCE, CREATOR>(this);
    RegisterSingleValueFloat<INSTANCE, CREATOR>(this);
    RegisterSingleValueDouble<INSTANCE, CREATOR>(this);
    RegisterSingleValueUInt128<INSTANCE, CREATOR>(this);

    RegisterSpecialMultiValueLine<INSTANCE, CREATOR>(this);
    RegisterSpecialMultiValuePolygon<INSTANCE, CREATOR>(this);
    RegisterSpecialMultiValueLocation<INSTANCE, CREATOR>(this);

    RegisterSpecialSingleValueTime<INSTANCE, CREATOR>(this);
    RegisterSpecialSingleValueTimestamp<INSTANCE, CREATOR>(this);
    RegisterSpecialSingleValueDate<INSTANCE, CREATOR>(this);

    RegisterAttributeString<INSTANCE, CREATOR>(this);
}

template <class INSTANCE, class CREATOR>
void AttributeFactory<INSTANCE, CREATOR>::RegisterSingleValueCreator(std::unique_ptr<CREATOR> creator)
{
    assert(creator != NULL);
    FieldType type = creator->GetAttributeType();
    std::lock_guard<std::mutex> guard(_mutex);
    _singleValueCreators[type] = std::move(creator);
}

template <class INSTANCE, class CREATOR>
void AttributeFactory<INSTANCE, CREATOR>::RegisterMultiValueCreator(std::unique_ptr<CREATOR> creator)
{
    assert(creator != NULL);
    FieldType type = creator->GetAttributeType();
    std::lock_guard<std::mutex> guard(_mutex);
    _multiValueCreators[type] = std::move(creator);
}

template <class INSTANCE, class CREATOR>
CREATOR* AttributeFactory<INSTANCE, CREATOR>::GetAttributeInstanceCreator(
    const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    std::shared_ptr<config::AttributeConfig> attrConfig =
        std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(nullptr != attrConfig);
    auto fieldConfig = attrConfig->GetFieldConfig();
    assert(nullptr != fieldConfig);
    bool isMultiValue = fieldConfig->IsMultiValue();
    FieldType fieldType = fieldConfig->GetFieldType();
    const std::string& fieldName = fieldConfig->GetFieldName();
    if (!isMultiValue) {
        if (fieldName == indexlib::MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME ||
            fieldName == indexlib::SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME) {
            // return CreateJoinDocidAttributeReader();
            assert(false); // TODO
            return nullptr;
        }
        std::lock_guard<std::mutex> guard(_mutex);
        auto it = _singleValueCreators.find(fieldType);
        if (it != _singleValueCreators.end()) {
            return it->second.get();
        } else {
            AUTIL_LOG(ERROR, "unsupport single value reader type, fieldType[%d]", fieldType);
            return nullptr;
        }
    } else {
        std::lock_guard<std::mutex> guard(_mutex);
        auto it = _multiValueCreators.find(fieldType);
        if (it != _multiValueCreators.end()) {
            return it->second.get();
        } else {
            AUTIL_LOG(ERROR, "unsupport multi value reader type, fieldType[%d]", fieldType);
            return nullptr;
        }
    }
    return nullptr;
}

} // namespace indexlibv2::index
