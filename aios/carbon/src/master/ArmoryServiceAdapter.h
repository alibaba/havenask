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
#ifndef CARBON_ARMORYSERVICEADAPTER_H
#define CARBON_ARMORYSERVICEADAPTER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ServiceAdapter.h"
#include "master/ServiceNodeManager.h"
#include "master/HttpClient.h"
#include "common/ArmoryAPI.h"
#include "autil/legacy/jsonizable.h"

BEGIN_CARBON_NAMESPACE(master);

JSONIZABLE_CLASS(ArmoryServiceConfig)
{
public:
    JSONIZE() {
        JSON_FIELD(host);
        JSON_FIELD(group);
        JSON_FIELD(buffGroup);
        JSON_FIELD(appUseType);
        JSON_FIELD(app);
        JSON_FIELD(returnAppUseType);
        json.Jsonize("owner", owner, std::string("hippo"));
    }

    std::string host; // armory api host
    std::string group; // dst armory group
    std::string buffGroup; // dst buffer group to return ip
    std::string appUseType; // optional
    std::string app; // used to create a group, optional
    std::string owner; // used to create a group, optional
    std::string returnAppUseType; // optional 
};
    
class ArmoryServiceAdapter : public ServiceAdapter
{
public:
    ArmoryServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                         const std::string &id = "");
    ~ArmoryServiceAdapter();
private:
    ArmoryServiceAdapter(const ArmoryServiceAdapter &);
    ArmoryServiceAdapter& operator=(const ArmoryServiceAdapter &);
public:
    virtual bool doInit(const ServiceConfig &config);

    ServiceAdapterType getType() const {
        return ST_ARMORY;
    }

    std::string getServiceConfigStr() const {
        return autil::legacy::ToJsonString(_armoryServiceConfig, true);
    }

    virtual bool recover();
    
TESTABLE_BEGIN(private)
    virtual bool doAddNodes(const ServiceNodeMap &nodes);
    
    virtual bool doDelNodes(const ServiceNodeMap &nodes);
    
    virtual bool doGetNodes(ServiceNodeMap *nodes);
    
    virtual bool doUpdateNodes(const ServiceNodeMap &nodes)  { return true; }

    void setArmoryAPI(common::ArmoryAPIPtr api) { _armoryAPI = api; }

    std::string getReturnAppUseType() const;

TESTABLE_END()

private:
    void filterNodes(ServiceNodeMap *nodes);

private:
    ServiceNodeManagerPtr _serviceNodeManagerPtr;
    ArmoryServiceConfig _armoryServiceConfig;
    common::ArmoryAPIPtr _armoryAPI;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ArmoryServiceAdapter);

class EcsArmoryServiceAdapter : public ArmoryServiceAdapter {
public:
    EcsArmoryServiceAdapter(const ServiceNodeManagerPtr &serviceNodeManagerPtr,
                            const std::string &id = "")
        :ArmoryServiceAdapter(serviceNodeManagerPtr, id)
    {}
    ~EcsArmoryServiceAdapter(){}
    ServiceAdapterType getType() const {
        return ST_ECS_ARMORY;
    }
};

CARBON_TYPEDEF_PTR(EcsArmoryServiceAdapter);

END_CARBON_NAMESPACE(master);

#endif //CARBON_ARMORYSERVICEADAPTER_H
