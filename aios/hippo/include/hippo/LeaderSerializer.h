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
#ifndef HIPPO_LEADERSERIALIZER_H
#define HIPPO_LEADERSERIALIZER_H
#include <string>
#include <memory>
#include "hippo/DriverCommon.h"

namespace hippo {

class LeaderSerializer
{
public:
    LeaderSerializer(const std::string &fileName)
        : _fileName(fileName)
    {}
    virtual ~LeaderSerializer() {}
    
public:
    virtual bool read(std::string &content) const = 0;
    
    virtual bool write(const std::string &content) = 0;

    virtual bool checkExist(bool &isExist) = 0;
    
protected:
    const std::string _fileName;
};

HIPPO_TYPEDEF_PTR(LeaderSerializer);
};

#endif //HIPPO_LEADERSERIALIZER_H
