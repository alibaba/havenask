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
#include <unordered_map>

#include "indexlib/base/FieldType.h"
#include "orc/Type.hh"

namespace indexlibv2 { namespace config {
class OrcConfig;
}} // namespace indexlibv2::config

namespace indexlibv2::index {

class TypeUtils
{
public:
    static std::unique_ptr<orc::Type> MakeOrcType(FieldType ft, bool isMulti);
    static std::unique_ptr<orc::Type> MakeOrcTypeFromConfig(const config::OrcConfig* config,
                                                            const std::list<uint64_t>& fieldIds);
    static bool IsSupportedPrimitiveType(orc::TypeKind kind);
    static bool TypeSupported(const orc::Type* type);

private:
    static const std::unordered_map<FieldType, orc::TypeKind> primitiveTypeMapping;
};

} // namespace indexlibv2::index
