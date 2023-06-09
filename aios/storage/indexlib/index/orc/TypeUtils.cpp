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
#include "indexlib/index/orc/TypeUtils.h"

#include <numeric>

#include "autil/Log.h"
#include "indexlib/index/orc/OrcConfig.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(index, OrcStorageReader);

const std::unordered_map<FieldType, orc::TypeKind> TypeUtils::primitiveTypeMapping = {
    {ft_int8, orc::BYTE},           {ft_int16, orc::SHORT},          {ft_int32, orc::INT},
    {ft_int64, orc::LONG},          {ft_uint8, orc::UNSIGNED_BYTE},  {ft_uint16, orc::UNSIGNED_SHORT},
    {ft_uint32, orc::UNSIGNED_INT}, {ft_uint64, orc::UNSIGNED_LONG}, {ft_float, orc::FLOAT},
    {ft_double, orc::DOUBLE},       {ft_string, orc::STRING}};

std::unique_ptr<orc::Type> TypeUtils::MakeOrcType(FieldType ft, bool isMulti)
{
    auto it = primitiveTypeMapping.find(ft);
    if (it == primitiveTypeMapping.end()) {
        return std::unique_ptr<orc::Type>();
    }
    auto type = orc::createPrimitiveType(it->second);
    if (isMulti) {
        return orc::createListType(std::move(type));
    } else {
        return type;
    }
}

std::unique_ptr<orc::Type> TypeUtils::MakeOrcTypeFromConfig(const config::OrcConfig* config,
                                                            const std::list<uint64_t>& fieldList)
{
    std::vector<uint64_t> fieldIds;
    if (fieldList.size() == 0) {
        fieldIds.resize(config->GetFieldConfigs().size());
        std::iota(fieldIds.begin(), fieldIds.end(), 0);
    } else {
        std::copy(fieldList.begin(), fieldList.end(), std::back_inserter(fieldIds));
    }

    std::vector<std::unique_ptr<orc::Type>> fieldTypes;

    const auto& fields = config->GetFieldConfigs();

    for (size_t i = 0; i < fieldIds.size(); i++) {
        if (fieldIds[i] >= fields.size()) {
            AUTIL_LOG(ERROR, "field id [%lu] not found, field config count is %lu", fieldIds[i], fields.size());
            return nullptr;
        }

        const auto& field = fields[fieldIds[i]];
        auto orcType = TypeUtils::MakeOrcType(field->GetFieldType(), field->IsMultiValue());
        if (orcType.get() == nullptr) {
            AUTIL_LOG(ERROR, "convert field: fieldName[%s] fieldType[%s] isMultiValue[%d] to orc type failed",
                      field->GetFieldName().c_str(), config::FieldConfig::FieldTypeToStr(field->GetFieldType()),
                      field->IsMultiValue());
            return nullptr;
        }
        fieldTypes.emplace_back(std::move(orcType));
    }

    auto structType = orc::createStructType();
    for (size_t i = 0; i < fieldIds.size(); ++i) {
        structType->addStructField(fields[fieldIds[i]]->GetFieldName(), std::move(fieldTypes[i]));
    }
    return structType;
}

bool TypeUtils::IsSupportedPrimitiveType(orc::TypeKind kind)
{
    for (const auto& it : primitiveTypeMapping) {
        if (it.second == kind) {
            return true;
        }
    }
    return false;
}

bool TypeUtils::TypeSupported(const orc::Type* type)
{
    if (IsSupportedPrimitiveType(type->getKind())) {
        return true;
    }
    if (type->getKind() != orc::LIST) {
        return false;
    }
    return type->getSubtypeCount() == 1 && IsSupportedPrimitiveType(type->getSubtype(0)->getKind());
}

} // namespace indexlibv2::index
