#include "build_service/admin/test/FakeMultiClusterRealtimeSchemaListKeeper.h"

#include "build_service/config/test/FakeRealtimeSchemaListKeeper.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FakeMultiClusterRealtimeSchemaListKeeper);

FakeMultiClusterRealtimeSchemaListKeeper::FakeMultiClusterRealtimeSchemaListKeeper() {}

FakeMultiClusterRealtimeSchemaListKeeper::~FakeMultiClusterRealtimeSchemaListKeeper() {}

void FakeMultiClusterRealtimeSchemaListKeeper::init(const std::string& indexRoot,
                                                    const std::vector<std::string>& clusterNames, uint32_t generationId)
{
    for (const auto& clusterName : clusterNames) {
        auto keeper = std::make_shared<config::FakeRealtimeSchemaListKeeper>();
        keeper->init(indexRoot, clusterName, generationId);
        _schemaListKeepers.push_back(keeper);
    }
}

}} // namespace build_service::admin
