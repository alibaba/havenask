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
#ifndef HIPPO_LEADERSERIALIZERIMPL_H
#define HIPPO_LEADERSERIALIZERIMPL_H

#include "util/Log.h"
#include "common/common.h"
#include "common/LeaderState.h"
#include "hippo/LeaderSerializer.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class LeaderSerializerImpl : public LeaderSerializer
{
public:
    LeaderSerializerImpl(const std::string &fileName,
                         common::LeaderState *leaderState);
    ~LeaderSerializerImpl();
private:
    LeaderSerializerImpl(const LeaderSerializerImpl &);
    LeaderSerializerImpl& operator=(const LeaderSerializerImpl &);
public:
    /* override */ bool read(std::string &content) const;
    
    /* override */ bool write(const std::string &content);

    /* override */ bool checkExist(bool &bExist);

private:
    common::LeaderState *_leaderState;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_TYPEDEF_PTR(LeaderSerializerImpl);

END_HIPPO_NAMESPACE(sdk);

#endif //HIPPO_LEADERSERIALIZERIMPL_H
