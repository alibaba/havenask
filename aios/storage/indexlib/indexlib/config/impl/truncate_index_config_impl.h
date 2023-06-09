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
#ifndef __INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/truncate_index_property.h"
#include "indexlib/config/truncate_strategy.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class TruncateIndexConfigImpl
{
public:
    TruncateIndexConfigImpl();
    ~TruncateIndexConfigImpl();

public:
    const std::string& GetIndexName() const { return mIndexName; }
    void SetIndexName(const std::string& indexName) { mIndexName = indexName; }

    void AddTruncateIndexProperty(const TruncateIndexProperty& property) { mTruncIndexProperties.push_back(property); }

    const TruncateIndexPropertyVector& GetTruncateIndexProperties() const { return mTruncIndexProperties; }

    const TruncateIndexProperty& GetTruncateIndexProperty(size_t i) const
    {
        assert(i < mTruncIndexProperties.size());
        return mTruncIndexProperties[i];
    }

    TruncateIndexProperty& GetTruncateIndexProperty(size_t i)
    {
        assert(i < mTruncIndexProperties.size());
        return mTruncIndexProperties[i];
    }

    size_t GetTruncateIndexPropertySize() const { return mTruncIndexProperties.size(); }

private:
    std::string mIndexName;
    TruncateIndexPropertyVector mTruncIndexProperties;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexProperty);
DEFINE_SHARED_PTR(TruncateIndexConfigImpl);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_INDEX_CONFIG_IMPL_H
