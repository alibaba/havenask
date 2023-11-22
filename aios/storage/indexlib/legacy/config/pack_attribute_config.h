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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class PackAttributeConfig : public autil::legacy::Jsonizable, public indexlibv2::index::PackAttributeConfig
{
public:
    PackAttributeConfig(const std::string& attrName, const CompressTypeOption& compressType,
                        uint64_t defragSlicePercent, const std::shared_ptr<FileCompressConfig>& fileCompressConfig);
    ~PackAttributeConfig();

public:
    Status AddAttributeConfig(const AttributeConfigPtr& attributeConfig);
    const std::vector<AttributeConfigPtr>& GetAttributeConfigVec() const;
    void AssertEqual(const PackAttributeConfig& other) const;
    AttributeConfigPtr CreateAttributeConfig() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackAttributeConfig> PackAttributeConfigPtr;
typedef std::vector<PackAttributeConfigPtr> PackAttributeConfigVector;
class PackAttributeConfigIterator
{
public:
    PackAttributeConfigIterator(const PackAttributeConfigVector& configs) : mConfigs(configs) {}

    PackAttributeConfigVector::const_iterator Begin() const { return mConfigs.begin(); }

    PackAttributeConfigVector::const_iterator End() const { return mConfigs.end(); }

private:
    PackAttributeConfigVector mConfigs;
};
typedef std::shared_ptr<PackAttributeConfigIterator> PackAttributeConfigIteratorPtr;
}} // namespace indexlib::config
