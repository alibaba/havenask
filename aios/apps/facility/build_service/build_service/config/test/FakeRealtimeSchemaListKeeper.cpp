#include "build_service/config/test/FakeRealtimeSchemaListKeeper.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, FakeRealtimeSchemaListKeeper);

FakeRealtimeSchemaListKeeper::FakeRealtimeSchemaListKeeper() {}

FakeRealtimeSchemaListKeeper::~FakeRealtimeSchemaListKeeper() {}

void FakeRealtimeSchemaListKeeper::init(const std::string& indexRoot, const std::string& clusterName,
                                        uint32_t generationId)
{
    _clusterName = clusterName;
}
bool FakeRealtimeSchemaListKeeper::addSchema(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
{
    _schemaCache[schema->GetSchemaId()] = schema;
    return true;
}
bool FakeRealtimeSchemaListKeeper::getSchemaList(
    indexlib::schemaid_t beginSchemaId, indexlib::schemaid_t endSchemaId,
    std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>>& schemaList)
{
    schemaList.clear();
    for (auto [schemaId, tabletSchema] : _schemaCache) {
        if (schemaId >= beginSchemaId && schemaId <= endSchemaId) {
            schemaList.push_back(tabletSchema);
        }
    }
    return true;
}
std::shared_ptr<indexlibv2::config::TabletSchema>
FakeRealtimeSchemaListKeeper::getTabletSchema(indexlib::schemaid_t targetSchemaId)
{
    for (auto [schemaId, tabletSchema] : _schemaCache) {
        if (schemaId == targetSchemaId) {
            return tabletSchema;
        }
    }
    return nullptr;
}

}} // namespace build_service::config
