#pragma once
#include "build_service/common_define.h"
#include "build_service/config/MultiClusterRealtimeSchemaListKeeper.h"
#include "build_service/config/RealtimeSchemaListKeeper.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FakeMultiClusterRealtimeSchemaListKeeper : public config::MultiClusterRealtimeSchemaListKeeper
{
public:
    FakeMultiClusterRealtimeSchemaListKeeper();
    ~FakeMultiClusterRealtimeSchemaListKeeper();

    FakeMultiClusterRealtimeSchemaListKeeper(const FakeMultiClusterRealtimeSchemaListKeeper&) = delete;
    FakeMultiClusterRealtimeSchemaListKeeper& operator=(const FakeMultiClusterRealtimeSchemaListKeeper&) = delete;
    FakeMultiClusterRealtimeSchemaListKeeper(FakeMultiClusterRealtimeSchemaListKeeper&&) = delete;
    FakeMultiClusterRealtimeSchemaListKeeper& operator=(FakeMultiClusterRealtimeSchemaListKeeper&&) = delete;

public:
    void init(const std::string& indexRoot, const std::vector<std::string>& clusterNames,
              uint32_t generationId) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeMultiClusterRealtimeSchemaListKeeper);

}} // namespace build_service::admin
