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
#ifndef NAVI_CONTROLDATATYPE_H
#define NAVI_CONTROLDATATYPE_H

#include "navi/engine/Type.h"

namespace navi {

class ControlDataType : public Type {
public:
    ControlDataType()
        : Type(__FILE__, CONTROL_DATA_TYPE)
    {
    }
    ~ControlDataType() {
    }
public:
    TypeErrorCode serialize(TypeContext &ctx,
                            const DataPtr &data) const override;
    TypeErrorCode deserialize(TypeContext &ctx, DataPtr &data) const override;
};

}

#endif //NAVI_CONTROLDATATYPE_H
