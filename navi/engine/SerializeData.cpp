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
#include "navi/engine/SerializeData.h"
#include "navi/engine/Type.h"
#include "navi/log/NaviLogger.h"

namespace navi {

SerializeData::SerializeData()
{
}

SerializeData::~SerializeData() {
}

bool SerializeData::serialize(const NaviObjectLogger &_logger,
                              NaviPartId toPartId,
                              NaviPartId fromPartId,
                              const DataPtr &data,
                              bool eof,
                              const Type *dataType,
                              NaviPortData &serializeData)
{
    serializeData.set_to_part_id(toPartId);
    serializeData.set_from_part_id(fromPartId);
    serializeData.set_eof(eof);
    if (!data) {
        return true;
    }
    if (!dataType) {
        NAVI_LOG(ERROR, "null data type");
        return false;
    }
    if (!fillData(_logger, data, dataType, serializeData)) {
        return false;
    }
    return true;
}

bool SerializeData::fillData(const NaviObjectLogger &_logger,
                             const DataPtr &data,
                             const Type *dataType,
                             NaviPortData &serializeData)
{
    if (!serializeToStr(&_logger, dataType, data,
                        *serializeData.mutable_data()))
    {
        return false;
    }
    serializeData.set_type(dataType->getName());
    return true;
}

bool SerializeData::serializeToStr(const NaviObjectLogger *_logger,
                                   const Type *dataType,
                                   const DataPtr &data,
                                   std::string &result)
{
    const auto &typeName = dataType->getName();
    const auto &dataTypeId = data->getTypeId();
    if (typeName != CONTROL_DATA_TYPE && typeName != dataTypeId) {
        NAVI_LOG(ERROR, "type mismatch: expect[%s], actual[%s]",
                 typeName.c_str(), dataTypeId.c_str());
        return false;
    }
    auto pool = data->getPool();
    autil::DataBuffer buffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE,
                             pool.get());
    TypeContext ctx(buffer, pool);
    auto ec = dataType->serialize(ctx, data);
    if (ec != TEC_NONE) {
        if (ec == TEC_NOT_SUPPORT) {
            NAVI_LOG(ERROR, "type [%s] do not support serialize",
                     dataType->getName().c_str());
        } else {
            NAVI_LOG(ERROR, "serialize data failed, type [%s]",
                     dataType->getName().c_str());
        }
        return false;
    }
    result.assign(buffer.getData(), buffer.getDataLen());
    return true;
}

bool SerializeData::deserialize(const NaviObjectLogger &_logger,
                                const NaviPortData &serializeData,
                                const Type *dataType,
                                autil::mem_pool::Pool *queryPool,
                                const PoolPtr &pool,
                                DataPtr &dataPtr)
{
    return deserializeFromStr(&_logger, serializeData.data(), dataType,
                              queryPool, pool, dataPtr);
}

bool SerializeData::deserializeFromStr(const NaviObjectLogger *_logger,
                                       const std::string &dataStr,
                                       const Type *dataType,
                                       autil::mem_pool::Pool *queryPool,
                                       const PoolPtr &pool,
                                       DataPtr &dataPtr)
{
    if (!dataStr.empty()) {
        autil::DataBuffer buffer((void *)dataStr.data(), dataStr.size(), queryPool);
        TypeContext ctx(buffer, pool);
        auto ec = dataType->deserialize(ctx, dataPtr);
        if (ec != TEC_NONE) {
            if (ec == TEC_NOT_SUPPORT) {
                NAVI_LOG(ERROR, "type [%s] do not support deserialize",
                         dataType->getName().c_str());
            } else {
                NAVI_LOG(ERROR, "deserialize data failed, type [%s]",
                         dataType->getName().c_str());
            }
            return false;
        } else {
            return true;
        }
    }
    return true;
}

}

