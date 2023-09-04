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
#ifndef CARBON_COMMONDEFINE_H
#define CARBON_COMMONDEFINE_H

#include <string>
#include <assert.h>
#include <memory>
#include "hippo/DriverCommon.h"


namespace carbon {

typedef std::string groupid_t;
typedef std::string roleid_t;
typedef std::string roleguid_t;
typedef std::string version_t;
typedef std::string nodeid_t;
typedef std::string tag_t;
typedef std::string nodespec_t;
typedef std::map<std::string, std::string> KVMap;

/* type define for hippo wrapper */
typedef hippo::SlotId SlotId;
typedef hippo::Resource Resource;
typedef hippo::Priority Priority;
typedef hippo::CpusetMode CpusetMode;
typedef hippo::ResourceAllocateMode ResourceAllocateMode;
typedef hippo::SlotResource SlotResource;
typedef hippo::SlaveStatus SlaveStatus;
typedef hippo::ProcessStatus ProcessStatus;
typedef hippo::PackageInfo PackageInfo;
typedef hippo::ProcessInfo ProcessInfo;
typedef hippo::DataInfo DataInfo;
typedef hippo::ProcessContext ProcessContext;
typedef hippo::ReleasePreference ReleasePreference;
typedef hippo::SlotInfo SlotInfo;
typedef hippo::PackageStatus PackageStatus;
typedef hippo::DataStatus DataStatus;
typedef hippo::SlaveStatus SlaveStatus;
typedef hippo::ConstraintConfig ConstraintConfig;
/* end */

// const var
static const std::string WORKER_IDENTIFIER_FOR_CARBON = "WORKER_IDENTIFIER_FOR_CARBON";

static const std::string HEALTHCHECK_PAYLOAD_SIGNATURE = "signature";
static const std::string HEALTHCHECK_PAYLOAD_CUSTOMINFO = "customInfo";
static const std::string HEALTHCHECK_PAYLOAD_GLOBAL_CUSTOMINFO = "globalCustomInfo";
static const std::string HEALTHCHECK_PAYLOAD_SERVICEINFO = "serviceInfo";
static const std::string HEALTHCHECK_PAYLOAD_IDENTIFIER = "identifier";
static const std::string HEALTHCHECK_PAYLOAD_SCHEDULERINFO = "schedulerInfo";
static const std::string HEALTHCHECK_PAYLOAD_PROCESS_VERSION = "porcessVersion";
static const std::string HEALTHCHECK_PAYLOAD_PRELOAD = "preload";

static const std::string SERVICEINFO_PAYLOAD_CM2_TOPOINFO = "cm2_topo_info";

static const std::string ENV_PACKAGE_CHECKSUM = "packageCheckSum";

struct DriverOptions {
    enum SchType {
        SCH_GROUP = 0, SCH_ROLE, SCH_NODE,
    };
    SchType schType;

    DriverOptions(SchType schType_ = SCH_GROUP) : schType(schType_) 
    {}
};

}

typedef std::vector<carbon::SlotInfo> SlotInfoVect;
typedef std::map<carbon::SlotId, carbon::SlotInfo> SlotInfoMap;

#define JSONIZABLE_CLASS(cls) class cls : public autil::legacy::Jsonizable
#define JSONIZE() void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json)
#define JSON_FIELD(f) json.Jsonize(#f, f, f)
#define JSON_NAME_FIELD(name, f) json.Jsonize(name, f, f)

#endif
