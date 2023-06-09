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
#ifndef __INDEXLIB_KV_SEARCHER_H
#define __INDEXLIB_KV_SEARCHER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/result.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, KVReader);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(config, ValueConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);

namespace indexlibv2 {
namespace index {
class KVIndexReader;
}
namespace index {
class FieldValueExtractor;
}
} // namespace indexlibv2

namespace indexlib { namespace test {

class KVSearcher
{
public:
    KVSearcher();
    ~KVSearcher();

public:
    void Init(const partition::IndexPartitionReaderPtr& reader, config::IndexPartitionSchema* schema,
              const std::string& regionName);
    void Init(std::shared_ptr<indexlibv2::index::KVIndexReader> reader,
              std::shared_ptr<indexlib::config::KVIndexConfig> kvIndexConfig);

    ResultPtr Search(const std::string& key, uint64_t timestamp, TableSearchCacheType searchCacheType);

private:
    void FillPackResult(const ResultPtr& result, const autil::StringView& value, autil::mem_pool::Pool* pool);
    void FillResult(const ResultPtr& result, const indexlibv2::index::FieldValueExtractor& valueExtractor);
    std::string ConvertValueToString(const config::AttributeConfigPtr& attrConfig, const autil::StringView& value);

private:
    index::KVReaderPtr mReader;
    std::shared_ptr<indexlibv2::index::KVIndexReader> _kvReader;
    config::ValueConfigPtr mValueConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVSearcher);

}} // namespace indexlib::test

#endif //__INDEXLIB_KV_SEARCHER_H
