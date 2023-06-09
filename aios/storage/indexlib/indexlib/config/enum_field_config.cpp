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
#include "indexlib/config/enum_field_config.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {

IE_LOG_SETUP(config, EnumFieldConfig);
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

class EnumFieldConfigImpl
{
public:
    typedef std::vector<std::string> ValueVector;
    ValueVector mValidValues;
};

EnumFieldConfig::EnumFieldConfig() : EnumFieldConfig("", false) {}

EnumFieldConfig::EnumFieldConfig(const string& fieldName, bool multiValue)
    : FieldConfig(fieldName, ft_enum, multiValue)
    , mImpl(new EnumFieldConfigImpl())
{
}

void EnumFieldConfig::AddValidValue(const string& validValue) { mImpl->mValidValues.push_back(validValue); }

void EnumFieldConfig::AddValidValue(const ValueVector& validValues)
{
    std::copy(validValues.begin(), validValues.end(), std::back_inserter(mImpl->mValidValues));
}

const EnumFieldConfig::ValueVector& EnumFieldConfig::GetValidValues() const { return mImpl->mValidValues; }

void EnumFieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    FieldConfig::Jsonize(json);

    if (json.GetMode() == TO_JSON) {
        if (GetFieldType() == ft_enum && !mImpl->mValidValues.empty()) {
            json.Jsonize(FIELD_VALID_VALUES, mImpl->mValidValues);
        }
        const char* types[] = {"TEXT", "STRING",   "ENUM", "INTEGER", "FLOAT",    "LONG",
                               "TIME", "LOCATION", "CHAR", "ONLINE",  "PROPERTY", "UNKNOWN"};
        assert((size_t)GetFieldType() < sizeof(types) / sizeof(char*));
        string typeStr = types[GetFieldType()];
        json.Jsonize(string(FIELD_TYPE), typeStr);
    }
}

void EnumFieldConfig::AssertEqual(const FieldConfig& other) const
{
    IE_CONFIG_ASSERT_EQUAL(GetFieldId(), other.GetFieldId(), "FieldId not equal");
    IE_CONFIG_ASSERT_EQUAL(IsMultiValue(), other.IsMultiValue(), "MultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(GetFieldName(), other.GetFieldName(), "FieldName not equal");
    IE_CONFIG_ASSERT_EQUAL(GetFieldType(), other.GetFieldType(), "FieldType not equal");
    try {
        const auto& rhs = dynamic_cast<const EnumFieldConfig&>(other);
        IE_CONFIG_ASSERT_EQUAL(mImpl->mValidValues, rhs.mImpl->mValidValues, "ValieValues not equal");
    } catch (std::bad_cast& e) {
        AUTIL_LEGACY_THROW(util::AssertEqualException, e.what());
    }
}
}} // namespace indexlib::config
