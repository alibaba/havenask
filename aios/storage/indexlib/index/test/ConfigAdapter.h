#pragma once

#include <memory>

namespace indexlibv2 {

namespace config {
class ITabletSchema;
} // namespace config

namespace index {
class PrimaryKeyIndexConfig;
}

namespace index::test {

std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>
GetPKIndexConfig(const std::shared_ptr<config::ITabletSchema>& schema);

} // namespace index::test
} // namespace indexlibv2
