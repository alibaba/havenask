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
#ifndef CARBON_SERIALIZERCREATOR_H
#define CARBON_SERIALIZERCREATOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/Serializer.h"
#include "hippo/LeaderSerializerCreator.h"

BEGIN_CARBON_NAMESPACE(master);

class SerializerCreator
{
public:
    SerializerCreator(worker_framework::LeaderChecker *leaderChecker,
                      const std::string &serializerDir,
                      const std::string &backupDir = "");
    virtual ~SerializerCreator();
private:
    SerializerCreator(const SerializerCreator &);
    SerializerCreator& operator=(const SerializerCreator &);
public:
    virtual SerializerPtr create(const std::string &fileName,
                                 const std::string &id,
                                 bool compressed = false);
    virtual std::string getSerializerDir() const {
        return _serializerDir;
    }

public: // for test
    void setLeaderSerializerCreator(
            hippo::LeaderSerializerCreator *leaderSerializerCreator)
    {
        delete _leaderSerializerCreator;
        _leaderSerializerCreator = leaderSerializerCreator;
    }
private:
    hippo::LeaderSerializerCreator *_leaderSerializerCreator;
    worker_framework::LeaderChecker *_leaderChecker;
    std::string _serializerDir;
    std::string _backupDir;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SerializerCreator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_SERIALIZERCREATOR_H
