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

#include "autil/Log.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

namespace indexlibv2::config {

class RangeIndexConfig : public SingleFieldIndexConfig
{
public:
    RangeIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    RangeIndexConfig(const RangeIndexConfig& other);
    ~RangeIndexConfig();

public:
    void Check() const override;
    InvertedIndexConfig* Clone() const override;
    static std::string GetBottomLevelIndexName(const std::string& indexName);
    static std::string GetHighLevelIndexName(const std::string& indexName);
    std::shared_ptr<InvertedIndexConfig> GetBottomLevelIndexConfig();
    std::shared_ptr<InvertedIndexConfig> GetHighLevelIndexConfig();
    bool CheckFieldType(FieldType ft) const override;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
