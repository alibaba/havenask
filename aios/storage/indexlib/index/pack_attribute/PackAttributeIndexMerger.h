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
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"

namespace indexlibv2::index {
class PackAttributeConfig;

class PackAttributeIndexMerger : public MultiValueAttributeMerger<char>
{
public:
    PackAttributeIndexMerger();
    ~PackAttributeIndexMerger();

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;

private:
    std::shared_ptr<PackAttributeConfig> _packAttributeConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
