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
#ifndef NAVI_SERIALIZEDATA_H
#define NAVI_SERIALIZEDATA_H

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/NaviStream.pb.h"
#include "autil/Lock.h"

namespace navi {

struct SerializeData {
public:
    SerializeData();
    ~SerializeData();
public:
    static bool serialize(const NaviObjectLogger &_logger,
                          NaviPartId toPartId,
                          NaviPartId fromPartId,
                          const DataPtr &data,
                          bool eof,
                          const Type *dataType,
                          NaviPortData &serializeData);
    static bool serializeToStr(const NaviObjectLogger *_logger,
                               const Type *dataType,
                               const DataPtr &data,
                               std::string &result);
    static bool deserialize(const NaviObjectLogger &_logger,
                            const NaviPortData &serializeData,
                            const Type *dataType,
                            DataPtr &dataPtr);
    static bool deserializeFromStr(const NaviObjectLogger *_logger,
                                   const std::string &dataStr,
                                   const Type *dataType,
                                   DataPtr &dataPtr);
private:
    static bool fillData(const NaviObjectLogger &_logger,
                         const DataPtr &data, const Type *dataType,
                         NaviPortData &serializeData);
};

}

#endif //NAVI_SERIALIZEDATA_H
