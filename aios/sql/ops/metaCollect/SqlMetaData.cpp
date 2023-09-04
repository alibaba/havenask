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
#include "sql/ops/metaCollect/SqlMetaData.h"

#include <engine/TypeContext.h>
#include <memory>

#include "autil/DataBuffer.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"
#include "sql/common/Log.h"

namespace sql {

const std::string SqlMetaDataType::TYPE_ID = "sql.sql_meta_data_id";

SqlMetaDataType::SqlMetaDataType()
    : navi::Type(__FILE__, TYPE_ID) {}

navi::TypeErrorCode SqlMetaDataType::serialize(navi::TypeContext &ctx,
                                               const navi::DataPtr &data) const {
    auto *sqlMetaData = dynamic_cast<SqlMetaData *>(data.get());
    if (!sqlMetaData) {
        SQL_LOG(ERROR, "sql meta data is empty");
        return navi::TEC_NULL_DATA;
    }
    const auto &collector = sqlMetaData->getCollector();
    if (!collector) {
        SQL_LOG(ERROR, "null sql search info collector");
        return navi::TEC_NULL_DATA;
    }
    auto &dataBuffer = ctx.getDataBuffer();
    dataBuffer.write(*collector);
    return navi::TEC_NONE;
}

navi::TypeErrorCode SqlMetaDataType::deserialize(navi::TypeContext &ctx,
                                                 navi::DataPtr &data) const {
    auto collector = std::make_shared<SqlSearchInfoCollector>();
    auto &dataBuffer = ctx.getDataBuffer();
    dataBuffer.read(*collector);
    data = std::make_shared<SqlMetaData>(collector);
    return navi::TEC_NONE;
}

REGISTER_TYPE(SqlMetaDataType);

SqlMetaData::SqlMetaData(const SqlSearchInfoCollectorPtr &collector)
    : navi::Data(SqlMetaDataType::TYPE_ID)
    , _collector(collector) {}

SqlMetaData::~SqlMetaData() {}

const SqlSearchInfoCollectorPtr &SqlMetaData::getCollector() const {
    return _collector;
}

} // namespace sql
