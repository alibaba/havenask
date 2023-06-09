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
#include "indexlib/config/pack_attribute_config.h"

#include <memory>

#include "indexlib/base/Status.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/field_config.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/FloatInt8Encoder.h"
#include "indexlib/util/Fp16Encoder.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, PackAttributeConfig);

struct PackAttributeConfig::Impl {
    std::vector<AttributeConfigPtr> attributeConfigVec;
};

PackAttributeConfig::PackAttributeConfig(const string& attrName, const CompressTypeOption& compressType,
                                         uint64_t defragSlicePercent,
                                         const std::shared_ptr<FileCompressConfig>& fileCompressConfig)
    : indexlibv2::config::PackAttributeConfig(attrName, compressType, defragSlicePercent, fileCompressConfig)
    , mImpl(std::make_unique<Impl>())
{
    assert(attrName.size() > 0);
}

PackAttributeConfig::~PackAttributeConfig() {}

void PackAttributeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
        indexlibv2::config::PackAttributeConfig::Serialize(json);
    }
}

Status
PackAttributeConfig::AddAttributeConfig(const std::shared_ptr<indexlibv2::config::AttributeConfig>& attributeConfig)
{
    auto status = indexlibv2::config::PackAttributeConfig::AddAttributeConfig(attributeConfig);
    RETURN_IF_STATUS_ERROR(status, "add attr config failed");
    auto legacyAttrConfig = std::dynamic_pointer_cast<AttributeConfig>(attributeConfig);
    if (!legacyAttrConfig) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "attribute config[%s] is not legacy config",
                               attributeConfig->GetAttrName().c_str());
    }
    mImpl->attributeConfigVec.push_back(legacyAttrConfig);
    return Status::OK();
}

const vector<AttributeConfigPtr>& PackAttributeConfig::GetAttributeConfigVec() const
{
    return mImpl->attributeConfigVec;
}

void PackAttributeConfig::AssertEqual(const PackAttributeConfig& other) const
{
    auto status = indexlibv2::config::PackAttributeConfig::CheckEqual(other);
    THROW_IF_STATUS_ERROR(status);
}

AttributeConfigPtr PackAttributeConfig::CreateAttributeConfig() const
{
    FieldConfigPtr fieldConfig(new FieldConfig(GetAttrName(), ft_string, false, true, false));
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig);
    attrConfig->SetUpdatableMultiValue(IsUpdatable());
    attrConfig->SetFileCompressConfig(GetFileCompressConfig());
    auto status = attrConfig->SetCompressType(GetCompressType().GetCompressStr());
    THROW_IF_STATUS_ERROR(status);
    fieldConfig->SetDefragSlicePercent(GetDefragSlicePercent());
    attrConfig->SetDefragSlicePercent(GetDefragSlicePercent());
    return attrConfig;
}

}} // namespace indexlib::config
