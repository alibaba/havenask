/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/config/TabletSchema.h"

namespace build_service { namespace config {

class RealtimeSchemaListKeeper
{
public:
    RealtimeSchemaListKeeper();
    virtual ~RealtimeSchemaListKeeper();

    RealtimeSchemaListKeeper(const RealtimeSchemaListKeeper&) = delete;
    RealtimeSchemaListKeeper& operator=(const RealtimeSchemaListKeeper&) = delete;
    RealtimeSchemaListKeeper(RealtimeSchemaListKeeper&&) = delete;
    RealtimeSchemaListKeeper& operator=(RealtimeSchemaListKeeper&&) = delete;

public:
    // virtual for test
    virtual void init(const std::string& indexRoot, const std::string& clusterName, uint32_t generationId);
    virtual bool addSchema(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema);
    virtual bool getSchemaList(indexlib::schemaid_t beginSchemaId, indexlib::schemaid_t endSchemaId,
                               std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>>& schemaList);
    virtual std::shared_ptr<indexlibv2::config::TabletSchema> getTabletSchema(indexlib::schemaid_t schemaId);
    virtual bool updateSchemaCache();

public:
    std::string getClusterName() const { return _clusterName; }

protected:
    std::string _schemaListDir;
    std::string _clusterName;
    std::map<indexlib::schemaid_t, std::shared_ptr<indexlibv2::config::TabletSchema>> _schemaCache;

private:
    static const std::string REALTIME_SCHEMA_LIST;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RealtimeSchemaListKeeper);

}} // namespace build_service::config
