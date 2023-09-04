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
#ifndef CARBON_COMPRESSEDSERIALIZER_H
#define CARBON_COMPRESSEDSERIALIZER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/Serializer.h"

BEGIN_CARBON_NAMESPACE(master);

class CompressedSerializer : public Serializer
{
public:
    CompressedSerializer(hippo::LeaderSerializer *leaderSerializer,
                         const std::string &id);
    ~CompressedSerializer();
private:
    CompressedSerializer(const CompressedSerializer &);
    CompressedSerializer& operator=(const CompressedSerializer &);
public:
    /* override */
    bool read(std::string &content) const;

    /* override */
    bool write(const std::string &content);

private:
    bool compress(const std::string &in, std::string &out) const;
    bool decompress(const std::string &in, std::string &out) const;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CompressedSerializer);

END_CARBON_NAMESPACE(master);

#endif //CARBON_COMPRESSEDSERIALIZER_H
