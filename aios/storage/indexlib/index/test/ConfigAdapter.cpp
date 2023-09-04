#include "indexlib/index/test/ConfigAdapter.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

namespace indexlibv2::index::test {

std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
GetPKIndexConfig(const std::shared_ptr<config::ITabletSchema>& schema)
{
    auto pkIndexConfigs = schema->GetIndexConfigs(index::PRIMARY_KEY_INDEX_TYPE_STR);
    if (pkIndexConfigs.size() != 1u) {
        return nullptr;
    }
    return std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(pkIndexConfigs[0]);
}

} // namespace indexlibv2::index::test
