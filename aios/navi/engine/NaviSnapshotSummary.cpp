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
#include "navi/engine/NaviSnapshotSummary.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/ResourceCreator.h"
#include <fstream>
#include <rapidjson/prettywriter.h>

namespace navi {

void RegistryInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (creator) {
        creator->Jsonize(json);
    }
}

void RegistrySummary::sort() {
    std::sort(infos.begin(), infos.end(),
              [](const RegistryInfo &lhs, const RegistryInfo &rhs) {
                  if (!lhs.creator || !rhs.creator) {
                      return true;
                  }
                  auto lhsResource = dynamic_cast<ResourceCreator *>(lhs.creator);
                  auto rhsResource = dynamic_cast<ResourceCreator *>(rhs.creator);
                  if (lhsResource && rhsResource) {
                      auto lhsStage = lhsResource->getStage();
                      auto rhsStage = rhsResource->getStage();
                      if (lhsStage != rhsStage) {
                          return lhsStage < rhsStage;
                      }
                  }
                  return lhs.creator->getName() < rhs.creator->getName();
              });
}

const std::string NaviSnapshotSummary::NAVI_VERSION = "navi-2.0.0";

void NaviSnapshotSummary::Jsonize(
    autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() != TO_JSON) {
        NAVI_KERNEL_LOG(ERROR, "not supported");
        return;
    }
    json.Jsonize("stage", stage);
    if (config) {
        json.Jsonize("config", *config);
    }
    json.Jsonize("biz_summary", bizs);
    json.Jsonize("version", NAVI_VERSION);
}

void NaviSnapshotSummary::logSummary(const NaviSnapshotSummary &summary) {
    std::string summaryStr;
    if (!NaviConfig::prettyDumpToStr(summary, summaryStr)) {
        NAVI_KERNEL_LOG(ERROR,
                        "log navi snapshot summary failed, can't dump summary");
        return;
    }
    const auto &naviName = summary.naviName;
    std::string fileName;
    if (!naviName.empty()) {
        fileName = "./navi." + naviName + ".snapshot_summary";
    } else {
        fileName = "./navi.snapshot_summary";
    }
    writeFile(fileName, summaryStr);
}

void NaviSnapshotSummary::writeFile(const std::string &fileName,
                                    const std::string &content)
{
    std::ofstream out(fileName);
    if (!out.good()) {
        NAVI_KERNEL_LOG(ERROR, "write file [%s] content [%s] failed",
                        fileName.c_str(), content.c_str());
        return;
    }
    out << content;
    out.close();
}
}

