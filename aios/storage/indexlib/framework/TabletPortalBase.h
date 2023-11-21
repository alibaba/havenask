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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITabletPortal.h"

namespace indexlibv2::config {
class TabletOptions;
}

namespace indexlibv2::document {
class IDocumentParser;
}

namespace indexlibv2::framework {
class ITablet;
class TabletInfos;

class TabletPortalBase : public ITabletPortal
{
public:
    TabletPortalBase();
    ~TabletPortalBase() override;

public:
    // when schemaName == "", will return all
    void GetTabletNames(const std::string& schemaName, std::vector<std::string>& tabletNames) const override;

    Status QueryTabletSchema(const std::string& tabletName, std::string& schemaStr) const override;

    Status QueryTabletInfoSummary(const std::string& tabletName, std::string& summaryInfo) const override;

    Status QueryTabletInfoDetail(const std::string& tabletName, std::string& tabletInfoStr) const override;

    Status SearchTablet(const std::string& tabletName, const std::string& jsonQuery,
                        std::string& result) const override;

    Status BuildRawDoc(const std::string& tabletName, const std::string& docStr, const std::string& docFormat) override;

    void SetAllowDuplicatedTabletName(bool flag) { _allowDupTabletName = flag; }
    void SetAllowBuildDoc(bool flag) { _allowBuildDoc = flag; }

private:
    Status GetTabletAndParser(const std::string& tabletName, std::shared_ptr<ITablet>& tablet,
                              std::shared_ptr<document::IDocumentParser>& parser) const;

    void FillTabletSummaryInfo(const TabletInfos* tabletInfo, std::map<std::string, std::string>& infoMap) const;

    void FillTabletOptionSummaryInfo(const std::shared_ptr<config::TabletOptions>& options,
                                     std::map<std::string, std::string>& infoMap) const;

private:
    volatile bool _allowDupTabletName = false; // allow=true for TEST
    volatile bool _allowBuildDoc = false;      // allow=true for TEST

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
