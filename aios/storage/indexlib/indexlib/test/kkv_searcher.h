#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/result.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, KKVReader);
DECLARE_REFERENCE_CLASS(index, KKVIterator);
DECLARE_REFERENCE_CLASS(common, PackAttributeFormatter);
DECLARE_REFERENCE_CLASS(config, ValueConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace test {

class KKVSearcher
{
public:
    KKVSearcher();
    ~KKVSearcher();

public:
    void Init(const partition::IndexPartitionReaderPtr& reader, config::IndexPartitionSchema* schema,
              const std::string& regionName);

    ResultPtr Search(const std::string& prefixKey, uint64_t timestamp, TableSearchCacheType searchCacheType);

    ResultPtr Search(const std::string& prefixKey, const std::vector<std::string>& suffixKeys, uint64_t timestamp,
                     TableSearchCacheType searchCacheType);

private:
    void FillResult(const ResultPtr& result, index::KKVIterator* iter);
    std::string GetSKeyValueStr(uint64_t skey);

private:
    index::KKVReaderPtr mReader;
    config::ValueConfigPtr mValueConfig;
    common::PackAttributeFormatterPtr mPackAttrFormatter;
    config::KKVIndexConfigPtr mKKVConfig;
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVSearcher);
}} // namespace indexlib::test
