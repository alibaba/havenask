#pragma once

#include "build_service/common_define.h"
#include "build_service/config/RealtimeSchemaListKeeper.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class FakeRealtimeSchemaListKeeper : public config::RealtimeSchemaListKeeper
{
public:
    FakeRealtimeSchemaListKeeper();
    ~FakeRealtimeSchemaListKeeper();

public:
    void init(const std::string& indexRoot, const std::string& clusterName, uint32_t generationId) override;
    bool addSchema(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema) override;
    bool getSchemaList(indexlib::schemaid_t beginSchemaId, indexlib::schemaid_t endSchemaId,
                       std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>>& schemaList) override;
    std::shared_ptr<indexlibv2::config::TabletSchema> getTabletSchema(indexlib::schemaid_t schemaId) override;
    bool updateSchemaCache() override { return true; }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeRealtimeSchemaListKeeper);

}} // namespace build_service::config
