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
#ifndef CARBON_GLOBAL_CONFIGMANAGER_H
#define CARBON_GLOBAL_CONFIGMANAGER_H

#include "common/common.h"
#include "carbon/CommonDefine.h"
#include "carbon/SerializerCreator.h"
#include "carbon/Serializer.h"
#include "master/GlobalConfig.h"
#include "autil/LoopThread.h"

BEGIN_CARBON_NAMESPACE(master);

class GlobalConfigManager 
{
public:
    GlobalConfigManager(carbon::master::SerializerCreatorPtr scp);
    virtual ~GlobalConfigManager();
    bool recover();
    bool setConfig(const GlobalConfig& conf, carbon::ErrorInfo* errorInfo);
    const GlobalConfig& getConfig() const;
    bool bgSyncFromZK();
    void prepareConfig(bool routeAll, const std::string& routeRegex);

protected: 
    bool syncFromZK();

private:
    mutable autil::ReadWriteLock _rwLock;
    GlobalConfig _config;
    carbon::master::SerializerPtr _serializerPtr;
    autil::LoopThreadPtr _syncThreadPtr;

private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(GlobalConfigManager);

END_CARBON_NAMESPACE(master);

#endif
