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
#ifndef MASTER_FRAMEWORK_COMMON_H
#define MASTER_FRAMEWORK_COMMON_H

#include <string>
#include <assert.h>
#include <memory>
#include "autil/CommonMacros.h"
#include "hippo/DriverCommon.h"

typedef std::vector<hippo::SlotResource> Resources;
typedef std::vector<hippo::ProcessInfo> ProcessInfos;
typedef std::vector<hippo::DataInfo> DataInfos;
typedef std::vector<hippo::PackageInfo> PackageInfos;
typedef std::vector<hippo::SlotInfo> SlotInfos;

#define BEGIN_MASTER_FRAMEWORK_NAMESPACE(x) namespace master_framework { namespace x {
#define END_MASTER_FRAMEWORK_NAMESPACE(x) } }
#define USE_MASTER_FRAMEWORK_NAMESPACE(x) using namespace master_framework::x
#define MASTER_FRAMEWORK_NS(x) master_framework::x
#define MF_NS(x) master_framework::x

#define MASTER_FRAMEWORK_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;

#define SCHEDULE_LOOPINTERVAL (1*1000*1000) //us, 1s
#define MF_ENV_ABNORMAL_TIMEOUT "MF_ENV_ABNORMAL_TIMEOUT"
#define SERIALIZE_DIR "serialize_dir"
#define CARBON_DIR "carbons"
#define GLOBAL_PLAN_PROP_SMOOTH_RECOVER "smooth_recover"
#define MF_MODE_FLAG "master_framework"
#define PROPERTY_TRUE_VALUE "true"
#define PROPERTY_FALSE_VALUE "false"
#define RECLAIM_WORKERNODE_TTL 60 * 60 * 1000 // ms, 1h

#endif
