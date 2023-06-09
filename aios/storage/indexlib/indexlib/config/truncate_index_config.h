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
#ifndef __INDEXLIB_TRUNCATE_INDEX_CONFIG_H
#define __INDEXLIB_TRUNCATE_INDEX_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/truncate_index_property.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, TruncateIndexConfigImpl);

namespace indexlib { namespace config {

class TruncateIndexConfig
{
public:
    TruncateIndexConfig();
    ~TruncateIndexConfig();

public:
    const std::string& GetIndexName() const;
    void SetIndexName(const std::string& indexName);
    void AddTruncateIndexProperty(const TruncateIndexProperty& property);

    const TruncateIndexPropertyVector& GetTruncateIndexProperties() const;

    const TruncateIndexProperty& GetTruncateIndexProperty(size_t i) const;

    TruncateIndexProperty& GetTruncateIndexProperty(size_t i);

    size_t GetTruncateIndexPropertySize() const;

private:
    TruncateIndexConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexProperty);
DEFINE_SHARED_PTR(TruncateIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_INDEX_CONFIG_H
