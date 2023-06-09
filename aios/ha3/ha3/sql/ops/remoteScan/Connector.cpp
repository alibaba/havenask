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
#include "ha3/sql/ops/remoteScan/Connector.h"

#include "autil/Log.h"
#include "ha3/sql/ops/remoteScan/RemoteScanProperties.h"
#include "ha3/sql/ops/remoteScan/TableServiceConnectorConfig.h"
#include "ha3/sql/ops/remoteScan/TableServiceConnector.h"

namespace isearch {
namespace sql {

AUTIL_DECLARE_AND_SETUP_LOGGER(sql, Connector);

Connector::ScanOptions::ScanOptions(const std::vector<std::string> &pks_,
                                    const std::vector<std::string> &attrs_,
                                    const std::vector<std::string> &attrsType_,
                                    const std::string &tableName_,
                                    const std::string &pkIndexName_,
                                    const std::string &serviceName_,
                                    const TableDistribution &tableDist_,
                                    const autil::mem_pool::PoolPtr &pool_,
                                    int64_t leftTime_)
    : pks(pks_)
    , attrs(attrs_)
    , attrsType(attrsType_)
    , tableName(tableName_)
    , pkIndexName(pkIndexName_)
    , serviceName(serviceName_)
    , tableDist(tableDist_)
    , pool(pool_)
    , leftTime(leftTime_)
{}

std::string Connector::ScanOptions::toString() {
    std::stringstream ss;
    ss << "pks: [";
    for (const auto &pk : pks) {
        ss << pk << ", ";
    }
    ss << "], attrs: [";
    for (const auto &attr : attrs) {
        ss << attr << ", ";
    }
    ss << "], tableName: [" << tableName << "]";
    return ss.str();
}

Connector::Connector() {}

Connector::~Connector() {}

std::unique_ptr<Connector> Connector::create(const std::string &sourceType)
{
    if (sourceType == "table_service") {
        return std::make_unique<TableServiceConnector>();
    } else {
        AUTIL_LOG(ERROR, "not support source type [%s]", sourceType.c_str());
        return nullptr;
    }
}

}
}
