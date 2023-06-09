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
#include "indexlib/framework/TabletPortalBase.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/TabletCenter.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletPortalBase);

TabletPortalBase::TabletPortalBase() : _allowDupTabletName(false), _allowBuildDoc(false) {}

TabletPortalBase::~TabletPortalBase() {}

void TabletPortalBase::GetTabletNames(const std::string& schemaName, std::vector<std::string>& tabletNames) const
{
    tabletNames.clear();
    std::vector<TabletCenter::TabletResource> tablets;
    if (schemaName.empty()) {
        TabletCenter::GetInstance()->GetAllTablets(tablets);
    } else {
        TabletCenter::GetInstance()->GetTabletBySchemaName(schemaName, tablets);
    }
    tabletNames.reserve(tablets.size());
    for (auto& tablet : tablets) {
        tabletNames.push_back(tablet.tabletPtr->GetTabletInfos()->GetTabletName());
    }
}

Status TabletPortalBase::QueryTabletSchema(const std::string& tabletName, std::string& schemaStr) const
{
    std::shared_ptr<ITablet> tablet;
    std::shared_ptr<document::IDocumentParser> parser;
    auto ret = GetTabletAndParser(tabletName, tablet, parser);
    if (!ret.IsOK()) {
        return ret;
    }
    assert(tablet);
    auto schema = tablet->GetTabletSchema();
    if (!schema) {
        return Status::InternalError("schema object is nullptr.");
    }

    if (!schema->Serialize(/*isCompact*/ false, &schemaStr)) {
        return Status::InternalError("schema Serialize fail.");
    }
    return Status::OK();
}

Status TabletPortalBase::QueryTabletInfoSummary(const std::string& tabletName, std::string& summaryInfo) const
{
    std::shared_ptr<ITablet> tablet;
    std::shared_ptr<document::IDocumentParser> parser;
    auto ret = GetTabletAndParser(tabletName, tablet, parser);
    if (!ret.IsOK()) {
        return ret;
    }
    assert(tablet);
    auto tabletInfo = tablet->GetTabletInfos();
    if (!tabletInfo) {
        return Status::InternalError("TabletInfo object is nullptr.");
    }

    std::map<std::string, std::string> summaryInfoMap;
    FillTabletSummaryInfo(tabletInfo, summaryInfoMap);
    FillTabletOptionSummaryInfo(tablet->GetTabletOptions(), summaryInfoMap);
    summaryInfo = autil::legacy::ToJsonString(summaryInfoMap);
    return Status::OK();
}

Status TabletPortalBase::QueryTabletInfoDetail(const std::string& tabletName, std::string& tabletInfoStr) const
{
    std::shared_ptr<ITablet> tablet;
    std::shared_ptr<document::IDocumentParser> parser;
    auto ret = GetTabletAndParser(tabletName, tablet, parser);
    if (!ret.IsOK()) {
        return ret;
    }
    assert(tablet);
    auto tabletInfo = tablet->GetTabletInfos();
    if (!tabletInfo) {
        return Status::InternalError("TabletInfo object is nullptr.");
    }

    std::map<std::string, std::string> summaryInfoMap;
    FillTabletSummaryInfo(tabletInfo, summaryInfoMap);
    auto tabletOptions = tablet->GetTabletOptions();
    FillTabletOptionSummaryInfo(tabletOptions, summaryInfoMap);

    std::map<std::string, std::string> metricsInfoMap;
    std::shared_ptr<Tablet> tabletTypeObj = std::dynamic_pointer_cast<Tablet>(tablet);
    if (tabletTypeObj) {
        auto tabletMetrics = tabletTypeObj->GetTabletInfos()->GetTabletMetrics();
        if (tabletMetrics) {
            tabletMetrics->FillMetricsInfo(metricsInfoMap);
        }
    }

    autil::legacy::json::JsonMap jsonMap;
    jsonMap["summary_info"] = autil::legacy::ToJson(summaryInfoMap);
    jsonMap["metrics_info"] = autil::legacy::ToJson(metricsInfoMap);

    jsonMap["version"] = autil::legacy::ToJson(tabletInfo->GetLoadedPublishVersion());
    auto counterMap = tabletInfo->GetCounterMap();
    if (counterMap) {
        jsonMap["counter_map"] = autil::legacy::json::ParseJson(counterMap->ToJsonString());
    }
    if (tabletOptions) {
        jsonMap["tablet_options"] = autil::legacy::ToJson(*tabletOptions);
    }
    tabletInfoStr = autil::legacy::json::ToString(jsonMap);
    return Status::OK();
}

Status TabletPortalBase::SearchTablet(const std::string& tabletName, const std::string& jsonQuery,
                                      std::string& result) const
{
    std::shared_ptr<ITablet> tablet;
    std::shared_ptr<document::IDocumentParser> parser;
    auto ret = GetTabletAndParser(tabletName, tablet, parser);
    if (!ret.IsOK()) {
        return ret;
    }
    assert(tablet);
    auto tabletReader = tablet->GetTabletReader();
    if (!tabletReader) {
        return Status::InternalError("tablet [%s] GetTabletReader return nullptr.", tabletName.c_str());
    }
    return tabletReader->Search(jsonQuery, result);
}

Status TabletPortalBase::BuildRawDoc(const std::string& tabletName, const std::string& docStr,
                                     const std::string& docFormat)
{
    if (!_allowBuildDoc) {
        return Status::InternalError("tablet [%s] not allow to build doc through tabletPortal.", tabletName.c_str());
    }

    std::shared_ptr<ITablet> tablet;
    std::shared_ptr<document::IDocumentParser> parser;
    auto ret = GetTabletAndParser(tabletName, tablet, parser);
    if (!ret.IsOK()) {
        return ret;
    }

    if (!parser) {
        return Status::InternalError("tablet [%s] with null document parser.", tabletName.c_str());
    }

    std::shared_ptr<indexlibv2::document::IDocumentBatch> docBatch(parser->Parse(docStr, docFormat).release());
    if (!docBatch) {
        return Status::InternalError("parse raw document fail, invalid doc string [%s]", docStr.c_str());
    }

    // TODO: write wal
    // TODO: set locator & timestamp in docBatch
    ret = tablet->Build(docBatch);
    if (!ret.IsOK()) {
        return ret;
    }
    return ret;
}

Status TabletPortalBase::GetTabletAndParser(const std::string& tabletName, std::shared_ptr<ITablet>& tablet,
                                            std::shared_ptr<document::IDocumentParser>& parser) const
{
    std::vector<TabletCenter::TabletResource> tablets;
    TabletCenter::GetInstance()->GetTabletByTabletName(tabletName, tablets);
    if (tablets.empty()) {
        return Status::NotFound("tablet [%s] not found.", tabletName.c_str());
    }

    if (tablets.size() > 1) {
        std::string errorStr =
            "find tablet [" + tabletName + "] number more than 1, find " + autil::StringUtil::toString(tablets.size());
        if (!_allowDupTabletName) {
            return Status::InternalError(errorStr.c_str());
        }
        std::cerr << errorStr << std::endl;
    }
    tablet = tablets[0].tabletPtr;
    parser = tablets[0].docParserPtr;
    return Status::OK();
}

void TabletPortalBase::FillTabletSummaryInfo(const TabletInfos* tabletInfo,
                                             std::map<std::string, std::string>& summaryInfoMap) const
{
    assert(tabletInfo);
    summaryInfoMap["tabletName"] = tabletInfo->GetTabletName();
    summaryInfoMap["memoryStatus"] = TabletInfos::MemoryStatusToStr(tabletInfo->GetMemoryStatus());
    summaryInfoMap["indexRoot"] = tabletInfo->GetIndexRoot().ToString();
    summaryInfoMap["docCount"] = autil::StringUtil::toString(tabletInfo->GetTabletDocCount());

    auto version = tabletInfo->GetLoadedPublishVersion();
    summaryInfoMap["versionId"] = autil::StringUtil::toString(version.GetVersionId());
    summaryInfoMap["versionTimestamp"] = autil::StringUtil::toString(version.GetTimestamp());
    summaryInfoMap["versionLocator"] = version.GetLocator().DebugString();
    summaryInfoMap["buildLocator"] = tabletInfo->GetBuildLocator().DebugString();
}

void TabletPortalBase::FillTabletOptionSummaryInfo(const std::shared_ptr<config::TabletOptions>& options,
                                                   std::map<std::string, std::string>& infoMap) const
{
    if (!options) {
        return;
    }

    infoMap["isLeader"] = options->IsLeader() ? "true" : "false";
    infoMap["isOnline"] = options->IsOnline() ? "true" : "false";
    infoMap["flushLocal"] = options->FlushLocal() ? "true" : "false";
    infoMap["flushRemote"] = options->FlushRemote() ? "true" : "false";
}

} // namespace indexlibv2::framework
