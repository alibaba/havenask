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
#ifndef NAVI_TYPE_H
#define NAVI_TYPE_H

#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/TypeContext.h"

namespace navi {

class Type : public Creator
{
public:
    Type(const std::string &typeId);
    virtual ~Type();
private:
    Type(const Type &);
    Type& operator=(const Type &);
public:
    const std::string &getName() const override {
        return _typeId;
    }
public:
    virtual TypeErrorCode serialize(TypeContext &ctx,
                                    const DataPtr &data) const;
    virtual TypeErrorCode deserialize(TypeContext &ctx, DataPtr &data) const;
private:
    std::string _typeId;
};

NAVI_TYPEDEF_PTR(Type);

#define REGISTER_TYPE(NAME) REGISTER_TYPE_HELPER(__COUNTER__, NAME)
#define REGISTER_TYPE_HELPER(ctr, NAME) REGISTER_TYPE_UNIQ(ctr, NAME)
#define REGISTER_TYPE_UNIQ(ctr, NAME)                                          \
    __attribute__((constructor)) static void Register##NAME##Type() {          \
        ::navi::CreatorRegistry::current(navi::RT_TYPE)                        \
            ->addCreatorFunc(                                                  \
                []() -> ::navi::Creator * { return new NAME(); });             \
    }
}

#endif //NAVI_TYPE_H
