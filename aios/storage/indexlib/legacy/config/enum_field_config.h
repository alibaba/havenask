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

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/field_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class EnumFieldConfigImpl;
typedef std::shared_ptr<EnumFieldConfigImpl> EnumFieldConfigImplPtr;
} // namespace indexlib::config

namespace indexlib { namespace config {

class EnumFieldConfig : public FieldConfig
{
public:
    typedef std::vector<std::string> ValueVector;

public:
    EnumFieldConfig();
    EnumFieldConfig(const std::string& fieldName, bool multiValue);
    ~EnumFieldConfig() {}

public:
    void AddValidValue(const std::string& validValue);
    void AddValidValue(const ValueVector& validValues);
    const ValueVector& GetValidValues() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const FieldConfig& other) const override;

private:
    EnumFieldConfigImplPtr mImpl;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EnumFieldConfig> EnumFieldConfigPtr;
}} // namespace indexlib::config
