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
#ifndef HIPPO_LEADERSERIALIZERCREATOR_H
#define HIPPO_LEADERSERIALIZERCREATOR_H

#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"

namespace hippo {

class LeaderSerializerCreator
{
public:
    LeaderSerializerCreator();
    virtual ~LeaderSerializerCreator();
private:
    LeaderSerializerCreator(const LeaderSerializerCreator &);
    LeaderSerializerCreator& operator=(const LeaderSerializerCreator &);
public:
    virtual LeaderSerializer* create(
            worker_framework::LeaderChecker * leaderChecker,
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "");
};

} //end of hippo namespace

#endif //HIPPO_LEADERSERIALIZERCREATOR_H
