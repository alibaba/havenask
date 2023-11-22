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
#include "sql/data/SqlQueryConfigData.h"

#include "autil/DataBuffer.h"

namespace sql {

const std::string SqlQueryConfigType::TYPE_ID = "sql_query_config_data_id";

SqlQueryConfigType::SqlQueryConfigType()
    : navi::Type(__FILE__, TYPE_ID) {}

SqlQueryConfigType::~SqlQueryConfigType() {}

navi::TypeErrorCode SqlQueryConfigType::serialize(navi::TypeContext &ctx,
                                                  const navi::DataPtr &data) const {
    auto *typedData = dynamic_cast<SqlQueryConfigData *>(data.get());
    if (!typedData) {
        return navi::TEC_NULL_DATA;
    }
    const auto &config = typedData->getConfig();
    auto &dataBuffer = ctx.getDataBuffer();
    dataBuffer.write(SQL_QUERY_CONFIG_DATA_CHECKSUM_BEGIN);
    size_t len = config.ByteSize();
    dataBuffer.write(len);
    void *buffer = dataBuffer.writeNoCopy(len);
    if (!config.SerializeToArray(buffer, len)) {
        assert(false && "sql query config data serialize failed");
    }
    dataBuffer.write(SQL_QUERY_CONFIG_DATA_CHECKSUM_END);
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlQueryConfigType::deserialize(navi::TypeContext &ctx,
                                                    navi::DataPtr &data) const {
    auto &dataBuffer = ctx.getDataBuffer();
    size_t checksum;
    dataBuffer.read(checksum);
    if (checksum != SQL_QUERY_CONFIG_DATA_CHECKSUM_BEGIN) {
        NAVI_KERNEL_LOG(ERROR,
                        "invalid checksum, expect [0x%lx] actual [0x%lx]",
                        SQL_QUERY_CONFIG_DATA_CHECKSUM_BEGIN,
                        checksum);
        return navi::TEC_FAILED;
    }
    size_t len;
    dataBuffer.read(len);
    const void *buffer = dataBuffer.readNoCopy(len);
    SqlQueryConfig config;
    if (!config.ParseFromArray(buffer, len)) {
        NAVI_KERNEL_LOG(ERROR, "sql query config parse from array failed, length [%lu]", len);
        return navi::TEC_FAILED;
    }

    dataBuffer.read(checksum);
    if (checksum != SQL_QUERY_CONFIG_DATA_CHECKSUM_END) {
        NAVI_KERNEL_LOG(ERROR,
                        "invalid checksum, expect [0x%lx] actual [0x%lx]",
                        SQL_QUERY_CONFIG_DATA_CHECKSUM_END,
                        checksum);
        return navi::TEC_FAILED;
    }
    data.reset(new SqlQueryConfigData(std::move(config)));
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlQueryConfigType);

SqlQueryConfigData::SqlQueryConfigData()
    : navi::Data(SqlQueryConfigType::TYPE_ID) {}

SqlQueryConfigData::SqlQueryConfigData(SqlQueryConfig config)
    : navi::Data(SqlQueryConfigType::TYPE_ID)
    , _config(std::move(config)) {}

SqlQueryConfigData::~SqlQueryConfigData() {}

const SqlQueryConfig &SqlQueryConfigData::getConfig() const {
    return _config;
}

} // namespace sql
