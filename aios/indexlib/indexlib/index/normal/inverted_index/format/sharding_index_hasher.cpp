#include "indexlib/index/normal/inverted_index/format/sharding_index_hasher.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/config/index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, ShardingIndexHasher);


void ShardingIndexHasher::Init(const IndexConfigPtr& indexConfig)
{
    assert(indexConfig->GetShardingType() == IndexConfig::IST_NEED_SHARDING);
    mHasher.reset(KeyHasherFactory::Create(indexConfig->GetIndexType()));
    mShardingCount = indexConfig->GetShardingIndexConfigs().size();
    mIsNumberIndex = IndexFormatOption::IsNumberIndex(indexConfig);
}

IE_NAMESPACE_END(index);

