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
#ifndef NAVI_TYPEINFO_H
#define NAVI_TYPEINFO_H

#include "navi/common.h"

namespace navi {

class TypeInfo
{
public:
    TypeInfo(const std::string &typeId);
    ~TypeInfo();
private:
    TypeInfo(const TypeInfo &);
    TypeInfo &operator=(const TypeInfo &);
public:
    const std::string &getTypeId() const;
private:
    std::string _typeId;
};

}

#endif //NAVI_TYPEINFO_H
