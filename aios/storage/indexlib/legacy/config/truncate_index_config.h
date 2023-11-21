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
#include "indexlib/config/truncate_index_property.h"
//#include "indexlib/config/truncate_strategy.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class TruncateIndexConfigImpl;
typedef std::shared_ptr<TruncateIndexConfigImpl> TruncateIndexConfigImplPtr;
} // namespace indexlib::config

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TruncateIndexProperty> TruncateIndexPropertyPtr;
typedef std::shared_ptr<TruncateIndexConfig> TruncateIndexConfigPtr;
}} // namespace indexlib::config
