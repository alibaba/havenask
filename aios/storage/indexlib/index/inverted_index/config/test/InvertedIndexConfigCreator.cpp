#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexConfigCreator);

std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig>
InvertedIndexConfigCreator::CreateSingleFieldIndexConfig(const std::string& indexName, InvertedIndexType indexType,
                                                         FieldType type, optionflag_t optionFlag)
{
    auto indexConfig = std::make_shared<indexlibv2::config::SingleFieldIndexConfig>(indexName, indexType);
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("field0", type, false);
    fieldConfig->SetFieldId(0);
    indexConfig->SetOptionFlag(optionFlag);
    auto status = indexConfig->SetFieldConfig(fieldConfig);
    assert(status.IsOK());
    if (indexType == it_number) {
        InvertedIndexType indexType = indexlibv2::config::InvertedIndexConfig::FieldTypeToInvertedIndexType(type);
        indexConfig->SetInvertedIndexType(indexType);
    }
    return indexConfig;
}

void InvertedIndexConfigCreator::CreateShardingIndexConfigs(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig, size_t shardingCount)
{
    const std::string& indexName = indexConfig->GetIndexName();
    indexConfig->SetShardingType(indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING);
    for (size_t i = 0; i < shardingCount; ++i) {
        std::shared_ptr<indexlibv2::config::InvertedIndexConfig> shardingIndexConfig(indexConfig->Clone());
        shardingIndexConfig->SetShardingType(indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING);
        shardingIndexConfig->SetIndexName(indexlibv2::config::InvertedIndexConfig::GetShardingIndexName(indexName, i));
        shardingIndexConfig->SetDictConfig(indexConfig->GetDictConfig());
        shardingIndexConfig->SetAdaptiveDictConfig(indexConfig->GetAdaptiveDictionaryConfig());
        shardingIndexConfig->SetFileCompressConfig(indexConfig->GetFileCompressConfig());
        indexConfig->AppendShardingIndexConfig(shardingIndexConfig);
    }
}

} // namespace indexlib::index
