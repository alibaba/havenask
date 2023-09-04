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
#pragma once

#include "autil/legacy/jsonizable.h"
#include "navi/config/NaviConfig.h"
#include "navi/log/NaviLogger.h"

namespace navi {

struct RegistryInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    Creator *creator = nullptr;
};

struct RegistrySummary : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        sort();
        json.Jsonize("count", infos.size());
        json.Jsonize("registrys", infos);
    }
    void sort();
public:
    std::vector<RegistryInfo> infos;
};

struct ModuleSummary : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("resource", resource);
        json.Jsonize("kernel", kernel);
        json.Jsonize("type", type);
    }
public:
    RegistrySummary resource;
    RegistrySummary kernel;
    RegistrySummary type;
};

struct KernelSet : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("count", set.size());
        json.Jsonize("list", set);
    }
    std::set<std::string> set;
};

struct ResourceLoadInfo : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("stage", stage);
    }
    std::string stage;
};

struct ResourceSetSummary : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("resource_count", infos.size());
        json.Jsonize("resource_info", infos);
    }
    std::map<std::string, ResourceLoadInfo> infos;
};

struct PartSummary : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("static_graph", staticGraphSummary);
        json.Jsonize("resource", resourceSet);
    }
    ResourceSetSummary resourceSet;
    std::map<std::string, std::string> staticGraphSummary;
};

struct BizSummary : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("module_summary", modules);
        json.Jsonize("part_ids", partIds);
        json.Jsonize("support_kernels", supportKernels);
        json.Jsonize("blacklist_kernels", blacklistKernels);
        json.Jsonize("unsupport_kernels", unsupportKernels);
        json.Jsonize("biz_resources", bizResources);
        json.Jsonize("part_resources", partResources);
    }
    std::map<std::string, ModuleSummary> modules;
    std::set<NaviPartId> partIds;
    KernelSet supportKernels;
    KernelSet blacklistKernels;
    KernelSet unsupportKernels;
    PartSummary bizResources;
    std::map<std::string, PartSummary> partResources;
};

struct NaviSnapshotSummary : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    static void logSummary(const NaviSnapshotSummary &summary);
    static void writeFile(const std::string &fileName, const std::string &content);
public:
    static const std::string NAVI_VERSION;
public:
    std::string naviName;
    std::string stage;
    const NaviConfig *config = nullptr;
    std::map<std::string, BizSummary> bizs;
};

}

