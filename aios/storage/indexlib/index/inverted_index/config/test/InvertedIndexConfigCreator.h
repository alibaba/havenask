#pragma once

#include "autil/Log.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlibv2::config {
class SingleFieldIndexConfig;
class InvertedIndexConfig;
} // namespace indexlibv2::config

namespace indexlib::index {

class InvertedIndexConfigCreator
{
public:
    static std::shared_ptr<indexlibv2::config::SingleFieldIndexConfig>
    CreateSingleFieldIndexConfig(const std::string& indexName = "TextIndex", InvertedIndexType indexType = it_text,
                                 FieldType type = ft_text, optionflag_t optionFlag = OPTION_FLAG_ALL);

    static void CreateShardingIndexConfigs(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                           size_t shardingCount);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
