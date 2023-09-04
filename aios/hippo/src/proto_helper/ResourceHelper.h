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
#ifndef HIPPO_RESOURCEHELPER_H
#define HIPPO_RESOURCEHELPER_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/proto/Common.pb.h"
#include "proto_helper/ResourceRequirement.h"

BEGIN_HIPPO_NAMESPACE(proto);
/*
  This class contains all functions for operating Resources
*/
class ResourceHelper
{
public:
    // prepare resources from string
    static Resource prepareResource(const std::string &description,
                                    const std::string &separator = ",");
    static SlaveResource prepareSlaveResource(const std::string &description,
            const std::string &separator = "#");
    static ResourceMap prepareResourceMap(const std::string &description,
            const std::string &separator = "#");
    static SlotResource prepareSlotResource(const std::string &description,
            const std::string &separator = "#");
    static std::string normalizeResources(const ResourceMap &resources);
    static std::string normalizeResources(
            const proto::SlotResource &slotResource);

    static std::string ResourceOptionsToString(const ResourceOptions &slotResource);

    // log resources
    static std::string toString(const proto::Resource &resource);
    static std::string toString(const ResourceMap &resources);
    static std::string toString(const proto::SlotResource &slotResource);
    static void logResource(const proto::Resource &resource);
    static void logResourceMap(const ResourceMap &resourceMap);
    static void logExclusiveResource(const StringSet &exclusives);
    static void logDiskResources(const DiskResources &diskResources);
    static void logSlotResource(const proto::SlotResource &slotResource);
    static void logSlaveResource(const proto::SlaveResource &slaveResource);

    static bool isDiskResource(const std::string &name);
    static bool isDiskSizeResource(const std::string &name);
    static bool isDiskPredictSizeResource(const std::string &name);
    static bool isDiskLimitSizeResource(const std::string &name);
    static bool isDiskIopsResource(const std::string &name);
    static bool isDiskRatioResource(const std::string &name);
    static bool isIpResource(const std::string &name);
    static bool isSnResource(const std::string &name);
    static bool isGpuResource(const std::string &name);
    static bool isFpgaResource(const std::string &name);
    static bool isPovResource(const std::string &name);
    static bool isNpuResource(const std::string &name);
    static bool isSlaveDetectedResource(const std::string &name);
    static bool isMasterReservedResource(const std::string &name);
    static bool isAnonymousResource(const std::string &resName);
    static bool isInternalResource(const std::string &name);
    static bool isInternalScalarResource(const std::string &name);
    static bool isScheduleResource(const proto::Resource &resource);
    static bool isTmpScheduleResource(const proto::Resource &resource);


    // convert resources
    static bool convertResourceType(const proto::Resource::Type &type,
                                    std::string *out, bool upperCase = true);

    static bool convertResourceType(const std::string &type,
                                    proto::Resource::Type *out);

    static proto::PackageInfo::PackageType convertPackageType(const std::string &type);

    static ResourceMap convertSlotResource(
            const proto::SlotResource &slotResource);

    static ResourceMap convertSlotResourceWithAdjustType(
            const proto::SlotResource &slotResource,
            const ResourceMap &resources);

    static ResourceMap convertSlotResourceWithAdjustType(
            const ResourceMap &require,
            const ResourceMap &resources);

    static void correctResourcesType(const ResourceMap &patterns,
            ResourceMap *resources);

    static proto::Resource* findResource(
            proto::SlotResource *slotResource,
            const std::string &resourceName);
    static proto::Resource* findDiskResource(
            proto::SlotResource *slotResource,
            const std::string &resourceName);

    static const proto::Resource* findDiskResource(
            const proto::ResourceMap &resources,
            const std::string &resourceName);

    static std::vector<proto::Resource> findAllDiskResource(const proto::ResourceMap &resMap,
        const std::string &resourceName);

    static ResourceMap getDiskRequirement(const proto::SlotResource &slotResource);

    static std::string getDiskResourceType(const proto::Resource &resource);

    static void getDiskAssignment(const proto::SlotResource &slotResource,
                                  std::string *name, int32_t *size,
                                  int32_t *iops, int32_t *ratio);
    static std::string generateDiskResourceName(const std::string &resourceName,
            const std::string &diskName);

    static std::string generateDiskResourceNameIfEmpty(const std::string &resourceName,
            const std::string &diskName);

    static std::string getDiskName(const std::string &diskResourceName);

    static std::string stripDiskName(const std::string &resourceName);
    static bool stripDiskName(Resource &resource, std::string &diskName);
    static void stripDiskName(ResourceMap &resources, std::string &diskName);
    static void joinDiskName(ResourceMap *resMap, const std::string &diskName);
    static std::string getDiskLocation(const ResourceMap &resMap);
    static std::string getDiskLocation(const proto::SlotResource &slotResource);

    static std::set<std::string> getDisks(const ResourceMap &resMap);
    static bool isDiskResourceMatch(const ResourceMap &resources,
                                    const ResourceMap &require,
                                    std::string *filterLabel = nullptr);
    static std::string getMaxDiskName(const ResourceMap &resources);

    // operating ip resources
    static int32_t getResourceTotalAmount(const ResourceMap &resources);
    static bool getIp(const std::string &ipResourceName, std::string *value);
    static bool getSubnetMask(const std::string &ipResourceName,
                              std::string *value);
    static bool getGateWay(const std::string &ipResourceName,
                           std::string *value);
    static bool getHostName(const std::string &ipResourceName,
                            std::string *value);
    static bool getHostNic(const std::string &ipResourceName,
                           std::string *value);
    static std::string generateIpResourceName(const std::string &ip);
    // return: ip_10.0.0.1:xx:xx:xxx
    static std::set<std::string> getIpResourceNames(const ResourceMap &resMap);
    static std::set<std::string> getGpuResourceNames(const ResourceMap &resMap);

    static ResourceMap getIpResources(const ResourceMap &resMap);
    static ResourceMap getGpuResources(const ResourceMap &resMap);
    static ResourceMap getFpgaResources(const ResourceMap &resMap);
    static ResourceMap getPovResources(const ResourceMap &resMap);
    static ResourceMap getNpuResources(const ResourceMap &resMap);
    // classify resources

    static void classifyResources(const ResourceMap &asks,
                                  DiskResources &diskResources,
                                  std::map<std::string, ResourceMap> &devResources,
                                  ResourceMap &restResMap);

    static void classifyResourcesByType(
            const ResourceMap &askResources,
            std::map<proto::Resource::Type, ResourceMap> &classifiedResources);

    static void filterOutDiskResource(
            const ResourceMap &resMap,
            DiskResources &diskResources);

    static void filterOutSpecialResources(
            const proto::SlotResource &requirement,
            ResourceMap &requiredDiskResMap,
            ResourceMap &requiredIpResMap,
            ResourceMap &requiredRestResMap);

    static void filterOutSpecialResources(
            const ResourceMap &requirement,
            ResourceMap &requiredDiskResMap,
            ResourceMap &requiredIpResMap,
            ResourceMap &requiredRestResMap);

    static void filterOutSpecialResources(
            const ResourceMap &requirement,
            ResourceMap &requiredDiskResMap,
            ResourceMap &requiredIpResMap,
            ResourceMap &requiredGpuResMap,
            ResourceMap &requiredFpgaResMap,
            ResourceMap &requiredRestResMap);

    static void getDiskResources(
            const ResourceMap &requirement,
            ResourceMap &requiredDiskResMap);

    static void addSlotResource(
            const proto::SlotResource &slotResource,
            std::map<std::string, proto::Resource> *usedGrantedResource);

    static bool hasInternalResource(const ResourceMap &resources);

    static void adjustInternalScalarResource(proto::ResourceRequest *request);
    static int32_t getResourceCountOfType(const ResourceMap &resources,
            const proto::Resource::Type &type);

    static ResourceMap getResourcesOfType(const ResourceMap &resources,
            const proto::Resource::Type &type);

    static void cutInternalResources(const ResourceMap &cut,
                                   ResourceMap &resources);
    static void addInternalResources(const ResourceMap &add,
                                   ResourceMap &resources);
    static void addDiskResources(const ResourceMap &add,
                                 ResourceMap &resources);

    static void addResources(ResourceMap &resources,
                             const ResourceMap &add);

    static void cutResources(ResourceMap &resources,
                             const ResourceMap &cut,
                             const bool removeItem = false);

    static bool isResourceMatch(const ResourceMap &resources,
                                const ResourceMap &require);


    static void cutRequirement(ResourceMap &require,
                               const ResourceMap &cut);

    static void extractScheduleResources(
            ResourceMap &needReclaimedResources,
            ResourceMap &exclusiveResources,
            ResourceMap &preferenceResources);

    static ResourceMap filterOutSpecifyTypeResources(
            const ResourceMap &resources,
            const Resource::Type type);

    static proto::SlotResource getTypeResources(
        const proto::SlotResource &requirement,
        const proto::Resource::Type &type);

    static proto::SlotResource getScoreResources(
        const proto::SlotResource &requirement);

    static void getSn(const std::string &snResourceName, std::string *value);

    static bool isResourceExist(const proto::SlaveResource &slaveResource,
                                const std::string &name);

    static bool isReservedResourceExist(const proto::SlaveResource &slaveResource,
            const std::string &name);

    static bool hasExcludeResource(const proto::SlaveResource &slaveResource);

    static bool hasQueueNameResource(const proto::SlaveResource &slaveResource);

    static int32_t roundPredictAmount(
            const int32_t require, const double predictLoad,
            const double roundPercent, const int32_t minAmount);
    static int32_t getResourceAmount(const proto::ResourceMap & resource,
				     const std::string & namePrefix);

    static int32_t getResourceAmount(const proto::SlotResource & resource,
				     const std::string & namePrefix);



    static bool sameSpecialResource(const std::string &require, const std::string &real);


    // judgement resources
private:
    static bool getIpInformation(const std::string &ipResourceName,
                                 int num, std::string *value);

private:

    HIPPO_LOG_DECLARE();
};

END_HIPPO_NAMESPACE(proto);

#endif //HIPPO_RESOURCEHELPER_H
