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
#include "indexlib/config/single_field_index_config.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SingleFieldIndexConfig);

struct SingleFieldIndexConfig::Impl {
    FieldConfigPtr fieldConfig;
    bool hasPrimaryKeyAttribute = false;
};

SingleFieldIndexConfig::SingleFieldIndexConfig(const string& indexName, InvertedIndexType indexType)
    : IndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
}

SingleFieldIndexConfig::SingleFieldIndexConfig(const SingleFieldIndexConfig& other)
    : IndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

SingleFieldIndexConfig::~SingleFieldIndexConfig() {}

uint32_t SingleFieldIndexConfig::GetFieldCount() const { return 1; }
void SingleFieldIndexConfig::Check() const
{
    IndexConfig::Check();
    CheckWhetherIsVirtualField();
}
void SingleFieldIndexConfig::CheckWhetherIsVirtualField() const
{
    bool valid = (IsVirtual() && mImpl->fieldConfig->IsVirtual()) || (!IsVirtual() && !mImpl->fieldConfig->IsVirtual());
    if (!valid) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "Index is virtual but field is not virtual or"
                             "Index is not virtual but field is virtual, index name [%s]",
                             GetIndexName().c_str());
    }
}

bool SingleFieldIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    return (mImpl->fieldConfig->GetFieldId() == fieldId);
}
void SingleFieldIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (IsVirtual()) {
        return;
    }
    IndexConfig::Jsonize(json);
    if (json.GetMode() == TO_JSON) {
        if (mImpl->fieldConfig.get() != NULL) {
            string fieldName = mImpl->fieldConfig->GetFieldName();
            json.Jsonize(INDEX_FIELDS, fieldName);
        }

        if (mImpl->hasPrimaryKeyAttribute) {
            json.Jsonize(HAS_PRIMARY_KEY_ATTRIBUTE, mImpl->hasPrimaryKeyAttribute);
        }
    } else {
        // parse HasPrimaryKeyAttribute
        if (GetInvertedIndexType() == it_primarykey64 || GetInvertedIndexType() == it_primarykey128) {
            bool flag = false;
            json.Jsonize(HAS_PRIMARY_KEY_ATTRIBUTE, flag, flag);
            SetPrimaryKeyAttributeFlag(flag);
        }
    }
}
void SingleFieldIndexConfig::AssertEqual(const IndexConfig& other) const
{
    IndexConfig::AssertEqual(other);
    const SingleFieldIndexConfig& other2 = (const SingleFieldIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->hasPrimaryKeyAttribute, other2.mImpl->hasPrimaryKeyAttribute,
                           "mImpl->hasPrimaryKeyAttribute not equal");
    if (mImpl->fieldConfig) {
        mImpl->fieldConfig->AssertEqual(*(other2.mImpl->fieldConfig));
    }
}
void SingleFieldIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }
IndexConfig* SingleFieldIndexConfig::Clone() const { return new SingleFieldIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> SingleFieldIndexConfig::ConstructConfigV2() const
{
    return DoConstructConfigV2<indexlibv2::config::SingleFieldIndexConfig>();
}

bool SingleFieldIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!IndexConfig::FulfillConfigV2(configV2)) {
        AUTIL_LOG(ERROR, "fulfill single field index config failed");
        return false;
    }
    auto typedConfigV2 = dynamic_cast<indexlibv2::config::SingleFieldIndexConfig*>(configV2);
    assert(typedConfigV2);
    if (mImpl->fieldConfig) {
        auto status = typedConfigV2->SetFieldConfig(mImpl->fieldConfig);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "construct single field index config failed[%s]", status.ToString().c_str());
            return false;
        }
    }
    return true;
}

IndexConfig::Iterator SingleFieldIndexConfig::CreateIterator() const
{
    return IndexConfig::Iterator(mImpl->fieldConfig);
}
int32_t SingleFieldIndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    fieldid_t fieldId = mImpl->fieldConfig->GetFieldId();
    return fieldId == id ? 0 : -1;
}
bool SingleFieldIndexConfig::CheckFieldType(FieldType ft) const
{
#define DO_CHECK(flag)                                                                                                 \
    if (flag)                                                                                                          \
        return true;

    InvertedIndexType it = GetInvertedIndexType();
    DO_CHECK(it == it_trie);

    DO_CHECK(ft == ft_text && it == it_text);
    DO_CHECK(ft == ft_uint64 && it == it_datetime);
    DO_CHECK(ft == ft_date && it == it_datetime);
    DO_CHECK(ft == ft_time && it == it_datetime);
    DO_CHECK(ft == ft_timestamp && it == it_datetime);
    DO_CHECK(ft == ft_string && (it == it_string));
    DO_CHECK((it == it_number_int8 && ft == ft_int8) || (it == it_number_uint8 && ft == ft_uint8) ||
             (it == it_number_int16 && ft == ft_int16) || (it == it_number_uint16 && ft == ft_uint16) ||
             (it == it_number_int32 && ft == ft_int32) || (it == it_number_uint32 && ft == ft_uint32) ||
             (it == it_number_int64 && ft == ft_int64) || (it == it_number_uint64 && ft == ft_uint64));
    DO_CHECK((it == it_number || it == it_range) &&
             (ft == ft_int8 || ft == ft_uint8 || ft == ft_int16 || ft == ft_uint16 || ft == ft_integer ||
              ft == ft_uint32 || ft == ft_long || ft == ft_uint64));
    return false;
}
void SingleFieldIndexConfig::SetFieldConfig(const FieldConfigPtr& fieldConfig)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType)) {
        stringstream ss;
        ss << "InvertedIndexType " << IndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()) << " and FieldType "
           << FieldConfig::FieldTypeToStr(fieldType) << " Mismatch. index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (fieldConfig->IsEnableNullField() &&
        (GetInvertedIndexType() == it_range || GetInvertedIndexType() == it_spatial)) {
        INDEXLIB_FATAL_ERROR(Schema, "InvertedIndexType [%s] not support enable null field [%s], index name [%s]",
                             IndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()),
                             fieldConfig->GetFieldName().c_str(), GetIndexName().c_str());
    }

    mImpl->fieldConfig = fieldConfig;
    SetAnalyzer(mImpl->fieldConfig->GetAnalyzerName());
}
FieldConfigPtr SingleFieldIndexConfig::GetFieldConfig() const { return mImpl->fieldConfig; }

// TODO: outsize used in plugin, expect merge to primarykey config
bool SingleFieldIndexConfig::HasPrimaryKeyAttribute() const { return mImpl->hasPrimaryKeyAttribute; }
void SingleFieldIndexConfig::SetPrimaryKeyAttributeFlag(bool flag) { mImpl->hasPrimaryKeyAttribute = flag; }
util::KeyValueMap SingleFieldIndexConfig::GetDictHashParams() const
{
    return util::ConvertFromJsonMap(mImpl->fieldConfig->GetUserDefinedParam());
}
std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> SingleFieldIndexConfig::GetFieldConfigs() const
{
    return {mImpl->fieldConfig};
}
}} // namespace indexlib::config
