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
#ifndef HIPPO_SERIALIZEABLE_H
#define HIPPO_SERIALIZEABLE_H

#include "util/Log.h"
#include "common/common.h"

BEGIN_HIPPO_NAMESPACE(common);

class Serializeable
{
public:
    Serializeable() {}
    virtual ~Serializeable() {}
public:
    virtual std::string serialize() const = 0;
    
    virtual bool deserialize(const std::string &content) = 0;
};

END_HIPPO_NAMESPACE(common);

#endif //HIPPO_SERIALIZEABLE_H
