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
#ifndef __INDEXLIB_DATE_INDEX_CONFIG_H
#define __INDEXLIB_DATE_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class DateIndexConfig : public SingleFieldIndexConfig
{
public:
    DateIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                    const std::string& defaultGranularity = "second");

    DateIndexConfig(const DateIndexConfig& other);
    ~DateIndexConfig();

public:
    typedef DateLevelFormat::Granularity Granularity;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;
    IndexConfig* Clone() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;
    Granularity GetBuildGranularity() const;
    DateLevelFormat GetDateLevelFormat() const;

public:
    void SetBuildGranularity(Granularity granularity);
    void SetDateLevelFormat(DateLevelFormat format);

private:
    std::string GranularityToString(Granularity granularity);
    Granularity StringToGranularity(const std::string& str);
    bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_DATE_INDEX_CONFIG_H
