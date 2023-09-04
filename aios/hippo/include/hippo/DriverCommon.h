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
#ifndef HIPPO_DRIVERCOMMON_H
#define HIPPO_DRIVERCOMMON_H

#include <string>
#include <vector>
#include <memory>
#include <map>
#include <iostream>
#include "autil/legacy/jsonizable.h"
#include "autil/StringUtil.h"

namespace hippo {

#define HIPPO_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
#define RELEASE_AND_RESET_PTR(x) (x)->release();(x).reset();

typedef std::map<std::string, std::string> MetaTagMap;

struct SlotId : public autil::legacy::Jsonizable {
    std::string k8sNamespace;
    std::string k8sPodName;
    std::string k8sPodUID;
    std::string slaveAddress;
    int32_t id;
    int64_t declareTime;
    int32_t restfulHttpPort;

    SlotId() {
        id = -1;
        declareTime = 0;
        restfulHttpPort = 10088;
    }

    bool operator < (const hippo::SlotId &other) const {
        if (this->slaveAddress == other.slaveAddress) {
            if (this->id == other.id) {
                return this->declareTime < other.declareTime;
            }
            return this->id < other.id;
        }
        return this->slaveAddress < other.slaveAddress;
    }

    bool operator == (const hippo::SlotId &other) const {
        if (this->slaveAddress == other.slaveAddress &&
            this->id == other.id && this->declareTime == other.declareTime)
        {
            return true;
        }
        return false;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("slaveAddress", slaveAddress, slaveAddress);
        json.Jsonize("id", id, id);
        json.Jsonize("declareTime", declareTime, declareTime);
    }
};

struct Resource : public autil::legacy::Jsonizable {
    enum Type {
        SCALAR = 0,
        TEXT = 1,
        EXCLUDE_TEXT = 2,
        QUEUE_NAME = 3,
        EXCLUSIVE = 4,
        PREFER_TEXT = 5,
        PROHIBIT_TEXT = 6,
        SCALAR_CMP = 7
    };
    Type type;
    std::string name;
    int32_t amount;

    Resource() {
        type = SCALAR;
        amount = 0;
    }

    Resource(const std::string &n, int32_t a, Type t = SCALAR) {
        type = t;
        name = n;
        amount = a;
    }

    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("name", name);
        json.Jsonize("amount", amount, 0);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string typeStr = convertResourceType(type);
            json.Jsonize("type", typeStr);
        } else {
            std::string typeStr;
            json.Jsonize("type", typeStr, std::string("SCALAR"));
            type = convertResourceType(typeStr);
        }
    }
    static std::string convertResourceType(Type type) {
        if (type == SCALAR) {
            return "SCALAR";
        } else if (type == TEXT) {
            return "TEXT";
        } else if (type == EXCLUDE_TEXT){
            return "EXCLUDE_TEXT";
        } else if (type == QUEUE_NAME) {
            return "QUEUE_NAME";
        } else if (type == EXCLUSIVE){
            return "EXCLUSIVE";
        } else if (type == PREFER_TEXT){
            return "PREFER_TEXT";
        } else if (type == PROHIBIT_TEXT){
            return "PROHIBIT_TEXT";
        } else if (type == SCALAR_CMP){
            return "SCALAR_CMP";
        }
        throw autil::legacy::NotJsonizableException(
                "invalid ResourceType: " +
                autil::StringUtil::toString(type));
    }

    static Type convertResourceType(const std::string &type) {
        if (type == "SCALAR") {
            return SCALAR;
        } else if (type == "TEXT") {
            return TEXT;
        } else if (type == "EXCLUDE_TEXT") {
            return EXCLUDE_TEXT;
        } else if (type == "QUEUE_NAME") {
            return QUEUE_NAME;
        } else if (type == "EXCLUSIVE"){
            return EXCLUSIVE;
        } else if (type == "PREFER_TEXT"){
            return PREFER_TEXT;
        } else if (type == "PROHIBIT_TEXT"){
            return PROHIBIT_TEXT;
        } else if (type == "SCALAR_CMP"){
            return SCALAR_CMP;
        }

        throw autil::legacy::NotJsonizableException(
                "invalid ResourceType: " + type);
    }
};

struct Priority : public autil::legacy::Jsonizable {
    enum Level {
        UNDEFINE = -1,
        SYSTEM = 0,
        PROD = 32, // product service
        NONPROD_MASTER = 64, // offline job's master
        NONPROD_WORKER = 128, // offline job's worker
        TEST = 256
    };
    int32_t majorPriority; // for preemption
    int32_t minorPriority; // for allocation sequence

    Priority() {
        majorPriority = 32;
        minorPriority = 0;
    }

    Priority(int32_t major, int32_t minor = 0) {
        majorPriority = major;
        minorPriority = minor;
    }

    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("major_priority", majorPriority);
        json.Jsonize("minor_priority", minorPriority);
    }
};

enum ResourceAllocateMode {
    MACHINE = 0,
    SLOT = 1,
    AUTO = 2
};

enum CpusetMode {
    UNDEFINE = -1,
    NONE = 0,
    SHARE = 1,
    RESERVED = 2,
    EXCLUSIVE = 3
};

typedef std::string queue_t;

struct SlotResource : public autil::legacy::Jsonizable {
    std::vector<Resource> resources;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("slotResources", resources, resources);
    }

    Resource* find(const std::string &name) {
        for (std::vector<Resource>::iterator it = resources.begin();
             it != resources.end(); ++it)
        {
            if (it->name == name) {
                return &*it;
            }
        }

        return NULL;
    }

    const Resource* find(const std::string &name) const {
        for (std::vector<Resource>::const_iterator it =
                 resources.begin(); it != resources.end(); ++it)
        {
            if (it->name == name) {
                return &*it;
            }
        }

        return NULL;
    }

    std::size_t size() const {
        return resources.size();
    }

    void add(const Resource &res) {
        resources.push_back(res);
    }
};

struct ConstraintConfig : public autil::legacy::Jsonizable {
    enum SpreadLevel {
        UNLIMIT = 0,
	HOST = 1,
	FRAME = 2,
	RACK = 3,
	ASW = 4,
	PSW = 5
    };
    SpreadLevel level;
    bool strictly;
    bool useHostWorkDir;
    int32_t maxInstancePerHost;
    int32_t maxInstancePerFrame;
    int32_t maxInstancePerRack;
    int32_t maxInstancePerASW;
    int32_t maxInstancePerPSW;
    std::vector<std::string> specifiedIps;
    std::vector<std::string> prohibitedIps;
    ConstraintConfig() :
        level(HOST), strictly(true), useHostWorkDir(true),
        maxInstancePerHost(1), maxInstancePerFrame(0), maxInstancePerRack(0),
        maxInstancePerASW(0), maxInstancePerPSW(0)
    {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("level", level, level);
        json.Jsonize("strictly", strictly, strictly);
        json.Jsonize("useHostWorkDir", useHostWorkDir, useHostWorkDir);
        json.Jsonize("maxInstancePerHost", maxInstancePerHost, maxInstancePerHost);
        json.Jsonize("maxInstancePerFrame", maxInstancePerFrame, maxInstancePerFrame);
        json.Jsonize("maxInstancePerRack", maxInstancePerRack, maxInstancePerRack);
        json.Jsonize("maxInstancePerASW", maxInstancePerASW, maxInstancePerASW);
        json.Jsonize("maxInstancePerPSW", maxInstancePerPSW, maxInstancePerPSW);
        json.Jsonize("specifiedIps", specifiedIps, specifiedIps);
        json.Jsonize("prohibitedIps", prohibitedIps, prohibitedIps);
    }
};

typedef std::string groupid_t;
typedef std::pair<std::string, std::string> PairType;

struct SlaveStatus {
    enum Status {
        UNKNOWN = 0,
        DEAD,
        ALIVE
    };

    Status status;

    SlaveStatus() {
        status = UNKNOWN;
    }
};

struct ProcessStatus {
    enum Status {
        PS_UNKNOWN = 0,
        PS_RUNNING,
        PS_RESTARTING,
        PS_STOPPING,
        PS_STOPPED,
        PS_FAILED,
        PS_TERMINATED
    };

    bool isDaemon;
    Status status;
    std::string processName;
    int32_t restartCount;
    int64_t startTime;
    int32_t exitCode;
    int32_t pid;
    int64_t instanceId;
    std::vector<PairType> otherInfos;

    ProcessStatus() {
        isDaemon = false;
        status = PS_UNKNOWN;
        restartCount = 0;
        startTime = 0;
        exitCode = 0;
        pid = -1;
        instanceId = 0;
    }
};

struct DataStatus {
    enum Status {
        UNKNOWN = 0,
        DEPLOYING,
        FINISHED,
        FAILED,
        STOPPED
    };
    enum ErrorCode {
        ERROR_NONE = 0,
        ERROR_SOURCE,
        ERROR_DEST,
        ERROR_OTHER
    };

    std::string name;
    std::string src;
    std::string dst;
    int32_t curVersion;
    int32_t targetVersion;
    Status status;
    int32_t retryCount;
    ErrorCode lastErrorCode;
    std::string lastErrorMsg;
    int64_t attemptId;

    DataStatus() {
        curVersion = -1;
        targetVersion = -1;
        status = UNKNOWN;
        retryCount = 0;
        lastErrorCode = ERROR_NONE;
        attemptId = 0;
    }
};

struct PackageStatus {
    enum Status {
        IS_UNKNOWN,
        IS_WAITING,
        IS_INSTALLING,
        IS_INSTALLED,
        IS_FAILED,
        IS_CANCELLED
    };
    Status status;
    PackageStatus() {
        status = IS_UNKNOWN;
    }
};

struct SlotPreference {
    enum PreferenceTag {
        PREF_NORMAL = 0,
        PREF_RELEASE = 1
    };
    PreferenceTag preftag;
    SlotPreference() {
        preftag = PREF_NORMAL;
    }
};

struct SlotInfo {
    // basic info
    std::string role;
    SlotId slotId;

    // changes to these fields will trigger resourceChange callback
    // master is reclaiming this slot, return as soon as possible
    bool reclaiming;

    // changes to these fields will trigger slotStatusChange callback
    SlotResource slotResource;

    // status from slave heart beat via master
    SlaveStatus slaveStatus;
    std::vector<ProcessStatus> processStatus;
    std::vector<DataStatus> dataStatus;
    PackageStatus packageStatus;
    PackageStatus preDeployPackageStatus;

    std::string packageChecksum;
    std::string preDeployPackageChecksum;
    int64_t launchSignature;

    bool noLongerMatchQueue;
    bool noLongerMatchResourceRequirement;

    std::string requirementId;

    SlotPreference slotPreference;

    std::string k8sNamespace;
    std::string k8sPodName;
    std::string k8sPodUID;
    std::string ip;

    SlotInfo() {
        reclaiming = false;
        noLongerMatchQueue = false;
        noLongerMatchResourceRequirement = false;
        launchSignature = 0;
    }
};

typedef std::shared_ptr<SlotInfo> SlotInfoPtr;

struct MasterInfo {
    std::string leaderAddress;
    bool connected;

    // last master related error
    int32_t lastErrorCode;
    std::string lastErrorMsg;

    MasterInfo() {
        connected = false;
        lastErrorCode = 0;
    }
};

struct PackageInfo : public autil::legacy::Jsonizable {
    enum PackageType {
        UNSUPPORTED = 1,
        RPM = 4,
        ARCHIVE = 5,
        IMAGE = 6
    };
    std::string URI;
    PackageType type;

    PackageInfo() {
        type = UNSUPPORTED;
    }

    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
    {
        json.Jsonize("packageURI", URI, URI);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string typeStr = convertPackageType(type);
            json.Jsonize("type", typeStr);
        } else {
            std::string typeStr;
            json.Jsonize("type", typeStr, std::string());
            type = convertPackageType(typeStr);
        }
    }

    static std::string convertPackageType(const PackageType &type) {
        switch(type) {
        case UNSUPPORTED:
            return "UNSUPPORTED";
        case RPM:
            return "RPM";
        case ARCHIVE:
            return "ARCHIVE";
        case IMAGE:
            return "IMAGE";
        default:
            return "";
        }
    }

    static PackageType convertPackageType(const std::string &type) {
        if (type == "UNSUPPORTED") {
            return UNSUPPORTED;
        } else if (type == "RPM") {
            return RPM;
        } else if (type == "ARCHIVE") {
            return ARCHIVE;
        } else if (type == "IMAGE") {
            return IMAGE;
        }
        return UNSUPPORTED;
    }

    friend std::ostream& operator<<(std::ostream &out, const PackageInfo &info) {
        out << info.URI;
        if (info.type == RPM) {
            out << "|RPM";
        } else if (info.type == ARCHIVE) {
            out << "|ARCHIVE";
        } else if (info.type == IMAGE) {
            out << "|IMAGE";
        } else {
            out << "|UNSUPPORTED";
        }
        return out;
    }
};

struct ProcessInfo : public autil::legacy::Jsonizable {
    bool isDaemon;
    std::string name;
    std::string cmd;
    std::vector<PairType> args;
    std::vector<PairType> envs;
    std::vector<PairType> otherInfos;
    int64_t instanceId;
    int64_t stopTimeout;
    int64_t restartInterval;
    int64_t restartCountLimit;
    int32_t procStopSig;

    ProcessInfo() {
        isDaemon = true;
        instanceId = 0;
        stopTimeout = 100;
        restartInterval = 10;
        restartCountLimit = 10;
        procStopSig = 10;
    }
    /* override */
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("isDaemon", isDaemon, isDaemon);
        json.Jsonize("processName", name, name);
        json.Jsonize("cmd", cmd, cmd);
        json.Jsonize("instanceId", instanceId, instanceId);
        json.Jsonize("args", args, args);
        json.Jsonize("envs", envs, envs);
        json.Jsonize("otherInfos", otherInfos, otherInfos);
        json.Jsonize("stopTimeout", stopTimeout, stopTimeout);
        json.Jsonize("restartInterval", restartInterval, restartInterval);
        json.Jsonize("restartCountLimit", restartCountLimit, restartCountLimit);
        json.Jsonize("procStopSig", procStopSig, procStopSig);
    }
};

struct DataInfo : public autil::legacy::Jsonizable {
    enum DataPathNormalizeType {
        NONE = 0,
        COLON_REPLACED = 1
    };

    std::string name;
    std::string src;
    std::string dst;
    int32_t version;
    int32_t retryCountLimit;
    int64_t attemptId;
    int64_t expireTime; // sec
    DataPathNormalizeType normalizeType;


    DataInfo() {
        version = -1;
        retryCountLimit = 0;
        attemptId = 0;
        expireTime = 259200; // dafault:3 day
        normalizeType = NONE;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
    {
        json.Jsonize("name", name);
        json.Jsonize("src", src);
        json.Jsonize("dst", dst);
        json.Jsonize("version", version, version);
        json.Jsonize("retryCountLimit", retryCountLimit, retryCountLimit);
        json.Jsonize("attemptId", attemptId, attemptId);
        json.Jsonize("expireTime", expireTime, expireTime);
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            std::string typeStr = convertNormalizeType(normalizeType);
            json.Jsonize("normalizeType", typeStr);
        } else {
            std::string typeStr;
            json.Jsonize("normalizeType", typeStr, std::string("NONE"));
            normalizeType = convertNormalizeType(typeStr);
        }
    }

    static std::string convertNormalizeType(const DataPathNormalizeType &type) {
        switch(type) {
        case NONE:
            return "NONE";
        case COLON_REPLACED:
            return "COLON_REPLACED";
        default:
            return "NONE";
        }
    }

    static DataPathNormalizeType convertNormalizeType(const std::string &type) {
        if (type == "NONE") {
            return NONE;
        } else if (type == "COLON_REPLACED") {
            return COLON_REPLACED;
        }
        return NONE;
    }
};

struct ProcessContext {
    std::vector<PackageInfo> pkgs;
    std::vector<PackageInfo> preDeployPkgs;
    std::vector<ProcessInfo> processes;
    std::vector<DataInfo> datas;
    std::string serviceVersion;
};

struct RoleRequest : public autil::legacy::Jsonizable {
    std::vector<SlotResource> options;
    std::vector<Resource> declare;
    std::string queue;
    ResourceAllocateMode allocateMode;
    int32_t count;
    Priority priority;
    groupid_t groupId;
    MetaTagMap metaTags;
    CpusetMode cpusetMode;
    ConstraintConfig constraints;
    std::vector<std::string> containerConfigs;
    std::string requirementId;
    std::string instanceGroup;
    ProcessContext context;
    RoleRequest() {
        count = 1;
        allocateMode = AUTO;
        groupId = "";
        cpusetMode = CpusetMode::RESERVED;
        requirementId = "";
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("declare", declare, declare);
        json.Jsonize("queue", queue, queue);
        json.Jsonize("count", count, count);
        json.Jsonize("priority", priority, priority);
        json.Jsonize("groupid", groupId, groupId);
        json.Jsonize("metatags", metaTags, metaTags);
        json.Jsonize("constraints", constraints, constraints);
        json.Jsonize("container_configs", containerConfigs, containerConfigs);
        json.Jsonize("requirement_id", requirementId, requirementId);
        json.Jsonize("instance_group", instanceGroup, instanceGroup);
        std::string allocateModeStr;
        std::string cpusetModeStr;
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            allocateModeStr = convertAllocateMode(allocateMode);
            json.Jsonize("allocateMode", allocateModeStr);
            json.Jsonize("require", options);
            cpusetModeStr = convertCpusetMode(cpusetMode);
            json.Jsonize("cpusetMode", cpusetModeStr);
        } else {
            json.Jsonize("allocateMode", allocateModeStr, std::string("AUTO"));
            json.Jsonize("cpusetMode", cpusetModeStr, std::string("RESERVED"));
            allocateMode = convertAllocateMode(allocateModeStr);
            cpusetMode = convertCpusetMode(cpusetModeStr);
            requireJsonize(json);
            if (count < 0) {
                throw autil::legacy::NotJsonizableException(
                        "invalid count: " + autil::StringUtil::toString(count));
            }
            for (std::vector<SlotResource>::iterator it = options.begin();
                 it != options.end(); ++it)
            {
                if (it->resources.empty()) {
                    throw autil::legacy::NotJsonizableException(
                            "empty resource require");
                }
            }
        }
    }

    void requireJsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        std::string exceptInfo;
        try {
            newFormatJsonize(json);
            return;
        } catch (const autil::legacy::ExceptionBase &e) {
            exceptInfo += e.what();
        }
        try {
            oldFormatJsonize(json);
            return;
        } catch (const autil::legacy::ExceptionBase &e) {
            exceptInfo += e.what();
        }
        try {
            json.Jsonize("require", options);
            return;
        } catch (const autil::legacy::ExceptionBase &e) {
            exceptInfo += e.what();
        }

        throw autil::legacy::NotJsonizableException(exceptInfo);
    }

    void newFormatJsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        typedef std::vector<std::vector<Resource> > ResMatric;
        ResMatric newOptions;
        json.Jsonize("require", newOptions);
        options.clear();

        for (ResMatric::iterator it = newOptions.begin();
             it != newOptions.end(); ++it)
        {
            SlotResource slotRes;
            slotRes.resources.swap(*it);
            options.push_back(slotRes);
        }
    }

    void oldFormatJsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        typedef std::map<std::string, int32_t> ResourceMap;
        typedef std::vector<ResourceMap> ResourceVec;
        ResourceVec oldReq;
        json.Jsonize("require", oldReq);
        options.clear();

        for (ResourceVec::const_iterator it = oldReq.begin();
             it != oldReq.end(); ++it)
        {
            SlotResource dest;
            const ResourceMap &src = *it;
            for (ResourceMap::const_iterator srcIt = src.begin();
                 srcIt != src.end(); ++srcIt)
            {
                Resource rc;
                rc.name = srcIt->first;
                rc.amount = srcIt->second;
                rc.type = Resource::SCALAR;
                dest.resources.push_back(rc);
            }
            options.push_back(dest);
        }
    }

    static std::string convertAllocateMode(ResourceAllocateMode mode) {
        if (mode == ResourceAllocateMode::MACHINE) {
            return "MACHINE";
        } else if (mode == ResourceAllocateMode::SLOT) {
            return "SLOT";
        } else if (mode == ResourceAllocateMode::AUTO) {
            return "AUTO";
        }
        throw autil::legacy::NotJsonizableException(
                "invalid ResourceAllocateMode: " +
                autil::StringUtil::toString(mode));
    }

    static ResourceAllocateMode convertAllocateMode(const std::string &mode) {
        if (mode == "MACHINE") {
            return ResourceAllocateMode::MACHINE;
        } else if (mode == "SLOT") {
            return ResourceAllocateMode::SLOT;
        } else if (mode == "AUTO") {
            return ResourceAllocateMode::AUTO;
        }
        throw autil::legacy::NotJsonizableException(
                "invalid ResourceAllocateMode: " + mode);
    }

    static std::string convertCpusetMode(CpusetMode mode) {
        if (mode == CpusetMode::NONE) {
            return "NONE";
        } else if (mode == CpusetMode::SHARE) {
            return "SHARE";
        } else if (mode == CpusetMode::RESERVED) {
            return "RESERVED";
        } else if (mode == CpusetMode::EXCLUSIVE) {
            return "EXCLUSIVE";
        }
        throw autil::legacy::NotJsonizableException(
                "invalid CpusetMode: " +
                autil::StringUtil::toString(mode));
    }

    static CpusetMode convertCpusetMode(const std::string &mode) {
        if (mode == "NONE") {
            return CpusetMode::NONE;
        } else if (mode == "SHARE") {
            return CpusetMode::SHARE;
        } else if (mode == "RESERVED") {
            return CpusetMode::RESERVED;
        } else if (mode == "EXCLUSIVE") {
            return CpusetMode::EXCLUSIVE;
        }
        throw autil::legacy::NotJsonizableException(
                "invalid CpusetMode: " + mode);
    }
};

struct ReleasePreference : public autil::legacy::Jsonizable {
    enum PreferenceType {
        RELEASE_PREF_ANY = 0,
        RELEASE_PREF_PREFER,
        RELEASE_PREF_BETTERNOT,
        RELEASE_PREF_PROHIBIT
    };
    PreferenceType type;
    uint32_t leaseMs;
    std::string workDirTag;

    ReleasePreference() {
        type = RELEASE_PREF_ANY;
        leaseMs = 0;
    }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("leaseMs", leaseMs);
        json.Jsonize("workDirTag", workDirTag, std::string());
        if (json.GetMode() == autil::legacy::Jsonizable::TO_JSON) {
            int32_t typeInt = (int32_t)type;
            json.Jsonize("type", typeInt);
        } else {
            int32_t typeInt = 0;
            json.Jsonize("type", typeInt, 0);
            type = PreferenceType(typeInt);
        }
    }

};

struct ErrorInfo {
    enum ErrorCode {
        ERROR_NONE = 0,
        // master error
        ERROR_MASTER_NOT_LEADER = 1,
        ERROR_MASTER_SERVICE_NOT_AVAILABLE = 2,
        ERROR_MASTER_SERIALIZE_FAIL = 3,
        ERROR_MASTER_APP_ALREADY_EXIST = 4,
        ERROR_MASTER_APP_NOT_EXIST = 5,
        ERROR_MASTER_SLAVE_ALREADY_EXIST = 6,
        ERROR_MASTER_SLAVE_NOT_EXIST = 7,
        ERROR_MASTER_OFFLINE_SLAVE = 8,
        ERROR_MASTER_UPDATE_SLAVE_INFO = 9,
        ERROR_MASTER_INVALID_REQUEST = 10,
        ERROR_MASTER_DESERIALIZE_FAIL = 11,
        ERROR_MASTER_UPDATE_APP_FAIL = 12,
        ERROR_MASTER_RESOURCE_CONFLICT = 13,
        ERROR_MASTER_RESOURCE_NOT_EXIST = 14,
        ERROR_MASTER_EXCLUDE_TEXT_CONFLICT = 15,
        ERROR_MASTER_QUEUE_ALREADY_EXIST = 16,
        ERROR_MASTER_QUEUE_NOT_EXIST = 17,
        ERROR_MASTER_QUEUE_RESOURCE_DESC_ERROR = 18,
        ERROR_MASTER_INNER_QUEUE_CANNT_DEL = 19,
        ERROR_MASTER_RULES_WRONG_FORMAT = 20,
        ERROR_MASTER_RULES_INVALID = 21,
        // resource adjust
        ERROR_RESOURCE_AJUST_NO_NEED_AJUST = 22,
        ERROR_RESOURCE_AJUST_FAIL_NOT_ENOUGH_RESOURCE = 23,
        ERROR_RESOURCE_AJUST_FAIL_RESOURCE_CONFLICT = 24,
        // slave error
        ERROR_SLAVE_CLEANUP_SLOT_PLAN = 25,
        ERROR_SLAVE_SLOTID_NOT_SPECIFIED = 26,
        ERROR_SLAVE_SLOT_NOT_EXIST = 27,
        // data error
        ERROR_DATA_SOURCE = 28,
        ERROR_DATA_DEST = 29,
        ERROR_DATA_OTHER = 30
    };
    ErrorCode errorCode;
    std::string errorMsg;

    ErrorInfo() {
        errorCode = ERROR_NONE;
        errorMsg = "";
    }
};

struct LaunchMeta {
    std::string requirementId;
    int64_t launchSignature;
    LaunchMeta(const std::string &reqId = "",
               int64_t launchSig = 0)
        : requirementId(reqId)
        , launchSignature(launchSig)
    {}
};

};

#endif //HIPPO_DRIVERCOMMON_H
