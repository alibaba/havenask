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
#include "master/ServiceNodeManager.h"
#include "master/ServiceAdapterCreator.h"
#include "master/SignatureGenerator.h"
#include "master/CM2ServiceAdapter.h"
#include "master/VIPServiceAdapter.h"
#include "master/ArmoryServiceAdapter.h"
#include "master/LVSServiceAdapter.h"
#include "master/SLBServiceAdapter.h"
#include "master/VPCSLBServiceAdapter.h"
#include "master/SkylineServiceAdapter.h"
#include "master/TunnelSLBServiceAdapter.h"
#include "carbon/Log.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ServiceAdapterCreator);

ServiceAdapterCreator::ServiceAdapterCreator(
         const SerializerCreatorPtr &serializerCreatorPtr, ServiceAdapterExtCreatorPtr extCreatorPtr)
{
    _serializerCreatorPtr = serializerCreatorPtr;
     _extCreatorPtr = extCreatorPtr;
}

ServiceAdapterCreator::~ServiceAdapterCreator() {
}

static map<ServiceAdapterType, string> typeMap = {
    {ST_CM2, "cm2"},
    {ST_VIP, "vip"},
    {ST_LVS, "lvs"},
    {ST_HSF, "hsf"},
    {ST_ARMORY, "armory"},
    {ST_SLB, "slb"},
    {ST_LIBRARY, "library"},
    {ST_VPC_SLB, "vpc_slb"},
    {ST_TUNNEL_SLB, "tunnel_slb"},
    {ST_ECS_ARMORY, "ecs_armory"},
    {ST_SKYLINE, "skyline"},
    {ST_ECS_SKYLINE, "ecs_skyline"},
};

ServiceNodeManagerPtr ServiceAdapterCreator::genServiceNodeManager(
        const string &serviceAdapterId, const ServiceConfig &config)
{
    string serializeFilePath = "service_nodes/" + serviceAdapterId;
    CARBON_LOG(INFO, "serializeFilePath for new serviceAdapter is: [%s]",
               serializeFilePath.c_str());
    SerializerPtr serializerPtr = _serializerCreatorPtr->create(
            serializeFilePath, serviceAdapterId);
    if (serializerPtr == NULL) {
        CARBON_LOG(ERROR, "create service adapter serializer failed. "
                   "serviceAdapterId:[%s].",
                   serviceAdapterId.c_str());
        return ServiceNodeManagerPtr();
    }
    
    ServiceNodeManagerPtr serviceNodeManagerPtr(
            new ServiceNodeManager(serializerPtr));
    return serviceNodeManagerPtr;
}

ServiceAdapterPtr ServiceAdapterCreator::create(
        const string &serviceAdapterId,
        const ServiceConfig &config)
{
    auto serviceAdapterPtr = doCreate(serviceAdapterId, config);
    
    if (serviceAdapterPtr && serviceAdapterPtr->init(config)) {
        CARBON_LOG(INFO, "create serviceAdapter with type %s",
                   typeMap[config.type].c_str());
        return serviceAdapterPtr;
    }
    
    return ServiceAdapterPtr();
}

ServiceAdapterPtr ServiceAdapterCreator::doCreate(
        const string &serviceAdapterId,
        const ServiceConfig &config)
{
    ServiceAdapterType type = config.type;
    ServiceAdapterPtr serviceAdapterPtr;
    if (type == ST_CM2) {
        serviceAdapterPtr.reset(new CM2ServiceAdapter(serviceAdapterId));
    } else {
        ServiceNodeManagerPtr serviceNodeManagerPtr = genServiceNodeManager(
                serviceAdapterId, config);
        if (serviceNodeManagerPtr == NULL)  {
            CARBON_LOG(ERROR, "create serviceNodeManager failed, config:[%s]",
                       ToJsonString(config, true).c_str());
            return ServiceAdapterPtr();
        }
        if (type == ST_VIP) {
            serviceAdapterPtr.reset(
                    new VIPServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_LVS) {
            serviceAdapterPtr.reset(
                    new LVSServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_ARMORY) {
            serviceAdapterPtr.reset(
                    new ArmoryServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_ECS_ARMORY) {
            serviceAdapterPtr.reset(
                    new EcsArmoryServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_SLB) {
            serviceAdapterPtr.reset(
                    new SLBServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_VPC_SLB) {
            serviceAdapterPtr.reset(
                    new VPCSLBServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_TUNNEL_SLB) {
            serviceAdapterPtr.reset(
                    new TunnelSLBServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_SKYLINE) {
            serviceAdapterPtr.reset(
                    new SkylineServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_ECS_SKYLINE) {
            serviceAdapterPtr.reset(
                    new EcsSkylineServiceAdapter(serviceNodeManagerPtr, serviceAdapterId));
        } else if (type == ST_LIBRARY) {
            if (!_extCreatorPtr) {
                CARBON_LOG(ERROR, "no extServiceAdapterCreator set");
                return ServiceAdapterPtr();
            }
            serviceAdapterPtr = _extCreatorPtr->create(serviceAdapterId, config);
        } else {
            CARBON_LOG(ERROR, "not support service adapter type:[%d], config:[%s].",
                       (int32_t)type, ToJsonString(config, true).c_str());
        }
    }

    return serviceAdapterPtr;
}


END_CARBON_NAMESPACE(master);

