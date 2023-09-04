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
#ifndef CARBON_SYSCONFIGMANAGER_H
#define CARBON_SYSCONFIGMANAGER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/CommonDefine.h"
#include "carbon/GlobalConfig.h"
#include "master/SerializerCreator.h"

BEGIN_CARBON_NAMESPACE(master);

class SysConfigManager 
{
public:
    SysConfigManager(SerializerCreatorPtr scp);

    bool recover();
    bool setConfig(const carbon::SysConfig& conf, ErrorInfo* errorInfo);
    carbon::SysConfig getConfig() const;

private:
    mutable autil::ReadWriteLock _rwLock;
    carbon::SysConfig _config;
    SerializerPtr _serializerPtr;
    carbon::SysConfig _allK8sConfig;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(SysConfigManager);

END_CARBON_NAMESPACE(master);

#endif
