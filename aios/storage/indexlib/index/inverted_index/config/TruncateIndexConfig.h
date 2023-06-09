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

#include "indexlib/index/inverted_index/config/TruncateIndexProperty.h"
#include "indexlib/index/inverted_index/config/TruncateStrategy.h"

namespace indexlibv2::config {

class TruncateIndexConfig
{
public:
    TruncateIndexConfig();
    ~TruncateIndexConfig();

public:
    const std::string& GetIndexName() const;
    void SetIndexName(const std::string& indexName);

    void AddTruncateIndexProperty(const TruncateIndexProperty& property);
    const std::vector<TruncateIndexProperty>& GetTruncateIndexProperties() const;
    const TruncateIndexProperty& GetTruncateIndexProperty(size_t i) const;
    size_t GetTruncateIndexPropertySize() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
