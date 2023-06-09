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

#include <functional>
#include <memory>

#include "indexlib/config/IIndexConfig.h"

namespace indexlibv2::config {

class IndexConfigHash
{
public:
    static size_t Hash(const IIndexConfig& indexConfig);

    static size_t Hash(const IIndexConfig* indexConfig) { return Hash(*indexConfig); }
    static size_t Hash(const std::shared_ptr<IIndexConfig>& indexConfig) { return Hash(*indexConfig); }
    static size_t Hash(const std::unique_ptr<IIndexConfig>& indexConfig) { return Hash(*indexConfig); }
};

} // namespace indexlibv2::config

namespace std {
template <>
struct hash<indexlibv2::config::IIndexConfig> {
    size_t operator()(const indexlibv2::config::IIndexConfig& indexConfig) const
    {
        return indexlibv2::config::IndexConfigHash::Hash(indexConfig);
    }
    size_t operator()(const indexlibv2::config::IIndexConfig* indexConfig) const
    {
        return indexlibv2::config::IndexConfigHash::Hash(indexConfig);
    }
    size_t operator()(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const
    {
        return indexlibv2::config::IndexConfigHash::Hash(indexConfig);
    }
    size_t operator()(const std::unique_ptr<indexlibv2::config::IIndexConfig>& indexConfig) const
    {
        return indexlibv2::config::IndexConfigHash::Hash(indexConfig);
    }
};
} // namespace std
