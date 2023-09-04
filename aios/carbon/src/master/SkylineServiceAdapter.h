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
#ifndef CARBON_SKYLINESERVICEADAPTER_H
#define CARBON_SKYLINESERVICEADAPTER_H
 
#include "common/common.h"
#include "carbon/Log.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"
#include "common/SkylineAPI.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

JSONIZABLE_CLASS(SkylineServiceConfig)
{
public:
    JSONIZE() {
        JSON_FIELD(host);
        JSON_FIELD(group);
        JSON_FIELD(buffGroup);
        JSON_FIELD(appUseType);
        JSON_FIELD(returnAppUseType);
        JSON_FIELD(key);
        JSON_FIELD(secret);
    }

    std::string host; // api host
    std::string group; // dst skyline group
    std::string buffGroup; // dst buffer group to return ip
    std::string appUseType; // optional
    std::string returnAppUseType; // optional 
    std::string key; // optional
    std::string secret; // optional
};

class SkylineServiceAdapter : public ServiceAdapter
{
public:
    SkylineServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                         const std::string &id = "");
    ~SkylineServiceAdapter();
private:
    SkylineServiceAdapter(const SkylineServiceAdapter &);
    SkylineServiceAdapter& operator=(const SkylineServiceAdapter &);
public:
    virtual bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_SKYLINE;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_skylineServiceConfig, true);
    }

    virtual bool recover();
    
TESTABLE_BEGIN(private)
    virtual bool doAddNodes(const ServiceNodeMap &nodes);
    
    virtual bool doDelNodes(const ServiceNodeMap &nodes);
    
    virtual bool doGetNodes(ServiceNodeMap *nodes);
    
    virtual bool doUpdateNodes(const ServiceNodeMap &nodes)  { return true; }

    void setSkylineAPI(common::SkylineAPIPtr api) { _skylineAPI = api; }

    std::string getReturnAppUseType() const;

TESTABLE_END()

private:
    void filterNodes(ServiceNodeMap *nodes);

private:
    ServiceNodeManagerPtr _serviceNodeManagerPtr;
    SkylineServiceConfig _skylineServiceConfig;
    common::SkylineAPIPtr _skylineAPI;
private:
    CARBON_LOG_DECLARE();
};

class EcsSkylineServiceAdapter : public SkylineServiceAdapter {
public:
    EcsSkylineServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                         const std::string &id) : SkylineServiceAdapter(serviceNodeManagerPtr, id) {}

    ServiceAdapterType getType() const {
        return ST_ECS_SKYLINE;
    }
};

CARBON_TYPEDEF_PTR(SkylineServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif 
