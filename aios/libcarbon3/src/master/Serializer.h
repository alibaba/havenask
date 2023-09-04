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
#ifndef CARBON_SERIALIZER_H
#define CARBON_SERIALIZER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "hippo/LeaderSerializer.h"

BEGIN_CARBON_NAMESPACE(master);

class Serializer
{
public:
    Serializer(hippo::LeaderSerializer *leaderSerializer, const std::string &id);
    virtual ~Serializer();
private:
    Serializer(const Serializer &);
    Serializer& operator=(const Serializer &);
public:
    virtual bool read(std::string &content) const;
    
    virtual bool write(const std::string &content);
    
    virtual bool checkExist(bool &bExist);

protected:
    std::string _id;
private:
    hippo::LeaderSerializer *_leaderSerializer;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(Serializer);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERIALIZER_H
