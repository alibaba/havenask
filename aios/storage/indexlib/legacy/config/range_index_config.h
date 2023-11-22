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
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class RangeIndexConfig : public SingleFieldIndexConfig
{
public:
    RangeIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    RangeIndexConfig(const RangeIndexConfig& other);
    ~RangeIndexConfig();

public:
    void Check() const override;
    IndexConfig* Clone() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;
    static std::string GetBottomLevelIndexName(const std::string& indexName);
    static std::string GetHighLevelIndexName(const std::string& indexName);
    IndexConfigPtr GetBottomLevelIndexConfig();
    IndexConfigPtr GetHighLevelIndexConfig();
    bool CheckFieldType(FieldType ft) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RangeIndexConfig> RangeIndexConfigPtr;
}} // namespace indexlib::config
