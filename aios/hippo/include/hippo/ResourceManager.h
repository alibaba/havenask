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
#ifndef HIPPO_RESOURCEMANAGER_H
#define HIPPO_RESOURCEMANAGER_H

#include "hippo/ResourceAllocator.h"
#include "worker_framework/LeaderChecker.h"

namespace hippo {

#define RESOURCE_MANAGER_START_TIMEOUT_TIME 10000 // 10s

class ResourceManager
{
protected:
    ResourceManager();
public:
    static ResourceManager* getInstance(
            const std::string &hippoZk,
            const std::string &appid,
            const std::string &serilizePath,
            const std::string &backupRoot,
            worker_framework::LeaderChecker *leaderChecker);
    static void destroyInstance(ResourceManager *resourceManager);
    virtual ~ResourceManager();
private:
    ResourceManager(const ResourceManager &);
    ResourceManager& operator=(const ResourceManager &);
public:
    // if bCreate = true, create one while not exist
    virtual ResourceAllocatorPtr getResourceAllocator(
            const std::string &groupName, bool bCreate = true) = 0;
    virtual void deleteResourceAllocator(const std::string &groupName) = 0;
    // begin to schedule resource, all unshared ResourceAllocator will be reclaimed 
    virtual bool start(bool isNewStart, int32_t waitWorkingTimeoutTime =
                       RESOURCE_MANAGER_START_TIMEOUT_TIME) = 0;
    
    virtual bool isWorking() = 0;
    // reclaim all unused resource
    virtual void cleanupUnusedResources() = 0;
    
    virtual void stop() = 0;
    // freeze resources' assignment
    virtual void freezeResources() = 0;
    // do resources' schedule
    virtual void unfreezeResources() = 0;

    // used for compatible update from old version sdk
    virtual bool serializeEmptyResourceAssignment() = 0;

    /* start() = init() + recover() + doStart()*/
    virtual bool init() = 0;
    virtual bool recover(bool isNewStart) = 0;
    virtual bool doStart(int32_t waitWorkingTimeoutTime =
                         RESOURCE_MANAGER_START_TIMEOUT_TIME) = 0;
    
    virtual hippo::ErrorInfo getLastErrorInfo() const = 0;
};

HIPPO_TYPEDEF_PTR(ResourceManager);

} //end of hippo namespace

#endif //HIPPO_RESOURCEMANAGER_H
