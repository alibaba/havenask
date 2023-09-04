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
#ifndef CARBON_WORKERNODECREATOR_H
#define CARBON_WORKERNODECREATOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/WorkerNode.h"
#include "master/IdGenerator.h"

BEGIN_CARBON_NAMESPACE(master);

class WorkerNodeCreator
{
public:
    WorkerNodeCreator(const std::string &baseId,
                      const IdGeneratorPtr &idGeneratorPtr = IdGeneratorPtr());
    virtual ~WorkerNodeCreator();
private:
    WorkerNodeCreator(const WorkerNodeCreator &);
    WorkerNodeCreator& operator=(const WorkerNodeCreator &);
public:
    virtual WorkerNodePtr create();
    
private:
    std::string _baseId;
    IdGeneratorPtr _idGeneratorPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(WorkerNodeCreator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_WORKERNODECREATOR_H
