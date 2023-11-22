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
#include "indexlib/framework/HttpTabletPortal.h"

#include <functional>
#include <iostream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/NetUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, HttpTabletPortal);

HttpTabletPortal::HttpTabletPortal(int32_t threadNum) : _port(-1), _hasError(false)
{
    _httpServer.new_task_queue = [threadNum] { return new httplib::ThreadPool(threadNum); };

    RegisterMethod_PrintTabletNames();
    RegisterMethod_QueryTabletSchema();
    RegisterMethod_QueryTabletInfoSummmary();
    RegisterMethod_QueryTabletInfoDetail();
    RegisterMethod_SearchTablet();
    RegisterMethod_BuildRawDoc();
    RegisterMethod_Stop();
}

HttpTabletPortal::~HttpTabletPortal() { Stop(); }

bool HttpTabletPortal::Start(const indexlib::util::KeyValueMap& param)
{
    bool enableBuildDoc = indexlib::util::GetTypeValueFromKeyValueMap(param, "allow_build", false);
    TabletPortalBase::SetAllowBuildDoc(enableBuildDoc);

    bool allowDuplicateTabletName =
        indexlib::util::GetTypeValueFromKeyValueMap(param, "tolerate_duplicate_tablet_name", false);
    TabletPortalBase::SetAllowDuplicatedTabletName(allowDuplicateTabletName);

    int port = indexlib::util::GetTypeValueFromKeyValueMap(param, "port", (int)-1);
    if (port > 0 && !_httpServer.bind_to_port("0.0.0.0", port)) {
        std::cerr << "HttpTabletPortal bind to port:" << port << " fail." << std::endl;
        AUTIL_LOG(ERROR, "bind to port [%d] fail.", port);
        return false;
    }

    if (port <= 0) {
        _port = _httpServer.bind_to_any_port("0.0.0.0"); // use random port
    } else {
        _port = port;
    }

    std::cerr << "HttpTabletPortal will listen to port:" << _port << std::endl;
    AUTIL_LOG(INFO, "HttpTabletPortal will listen to port [%d]", _port);

    std::string portInfoFile =
        indexlib::util::GetValueFromKeyValueMap(param, "address_file", "http_tablet_portal.info");
    if (!portInfoFile.empty()) {
        FILE* fp = fopen(portInfoFile.c_str(), "w");
        std::string ip = "0.0.0.0";
        autil::NetUtil::GetDefaultIp(ip);
        fprintf(fp, "%s:%d", ip.c_str(), port);
        fclose(fp);
    }

    auto listenAfterBind = [this]() {
        if (!_httpServer.listen_after_bind()) {
            _hasError = true;
        }
        std::cerr << "http server is not running" << std::endl;
        AUTIL_LOG(ERROR, "http server is not running");
    };
    _listenThread = autil::Thread::createThread(listenAfterBind, "HttpTabletPortal");
    if (!_listenThread) {
        std::cerr << "create HttpTabletPortal listen thread failed" << std::endl;
        AUTIL_LOG(ERROR, "create HttpTabletPortal listen thread failed");
        return false;
    }
    usleep(100 * 1000);
    return !_hasError;
}

void HttpTabletPortal::Stop()
{
    if (_httpServer.is_running()) {
        AUTIL_LOG(INFO, "stop HttpTabletPortal.");
    }
    _httpServer.stop();
}

bool HttpTabletPortal::IsRunning() const { return _httpServer.is_running(); }

void HttpTabletPortal::RegisterMethod_PrintTabletNames()
{
    auto printTabletNameFunc = [&](const httplib::Request& req, httplib::Response& res) {
        std::vector<std::string> tabletNames;
        std::string schemaName = req.has_param("schema_name") ? req.get_param_value("schema_name") : "";
        TabletPortalBase::GetTabletNames(schemaName, tabletNames);
        res.set_content(autil::legacy::ToJsonString(tabletNames), "text/plain");
    };

    _httpServer.Get("/print_tablet_names", printTabletNameFunc);
    _httpServer.Post("/print_tablet_names", printTabletNameFunc);
}

void HttpTabletPortal::RegisterMethod_QueryTabletSchema()
{
    auto queryTabletSchemaFunc = [&](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("tablet_name")) {
            res.set_content("no valid key [tablet_name]\n", "text/plain");
            return;
        }
        auto tabletName = req.get_param_value("tablet_name");
        std::string schemaStr;
        auto ret = TabletPortalBase::QueryTabletSchema(tabletName, schemaStr);
        if (ret.IsOK()) {
            res.set_content(schemaStr, "text/plain");
        } else {
            res.set_content(ret.ToString(), "text/plain");
        }
    };

    _httpServer.Get("/query_schema", queryTabletSchemaFunc);
    _httpServer.Post("/query_schema", queryTabletSchemaFunc);
}

void HttpTabletPortal::RegisterMethod_QueryTabletInfoSummmary()
{
    auto queryTabletInfoSummaryFunc = [&](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("tablet_name")) {
            res.set_content("no valid key [tablet_name]\n", "text/plain");
            return;
        }
        auto tabletName = req.get_param_value("tablet_name");
        std::string summaryInfo;
        auto ret = TabletPortalBase::QueryTabletInfoSummary(tabletName, summaryInfo);
        if (ret.IsOK()) {
            res.set_content(summaryInfo, "text/plain");
        } else {
            res.set_content(ret.ToString(), "text/plain");
        }
    };

    _httpServer.Get("/query_info_summary", queryTabletInfoSummaryFunc);
    _httpServer.Post("/query_info_summary", queryTabletInfoSummaryFunc);
}

void HttpTabletPortal::RegisterMethod_QueryTabletInfoDetail()
{
    auto queryTabletInfoDetailFunc = [&](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("tablet_name")) {
            res.set_content("no valid key [tablet_name]\n", "text/plain");
            return;
        }
        auto tabletName = req.get_param_value("tablet_name");
        std::string detailInfo;
        auto ret = TabletPortalBase::QueryTabletInfoDetail(tabletName, detailInfo);
        if (ret.IsOK()) {
            res.set_content(detailInfo, "text/plain");
        } else {
            res.set_content(ret.ToString(), "text/plain");
        }
    };

    _httpServer.Get("/query_info_detail", queryTabletInfoDetailFunc);
    _httpServer.Post("/query_info_detail", queryTabletInfoDetailFunc);
}

void HttpTabletPortal::RegisterMethod_SearchTablet()
{
    auto searchTabletFunc = [&](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("tablet_name")) {
            res.set_content("no valid key [tablet_name]\n", "text/plain");
            return;
        }

        auto tabletName = req.get_param_value("tablet_name");
        std::string query = req.has_param("query") ? req.get_param_value("query") : req.body;
        std::string result;
        auto ret = TabletPortalBase::SearchTablet(tabletName, query, result);
        if (ret.IsOK()) {
            res.set_content(result, "text/plain");
        } else {
            res.set_content(ret.ToString(), "text/plain");
        }
    };

    _httpServer.Get("/search_tablet", searchTabletFunc);
    _httpServer.Post("/search_tablet", searchTabletFunc);
}

void HttpTabletPortal::RegisterMethod_BuildRawDoc()
{
    auto buildDocFunc = [&](const httplib::Request& req, httplib::Response& res) {
        if (!req.has_param("tablet_name")) {
            res.set_content("no valid key [tablet_name]\n", "text/plain");
            return;
        }

        auto tabletName = req.get_param_value("tablet_name");
        std::string docStr = req.has_param("doc") ? req.get_param_value("doc") : req.body;
        std::string formatStr = req.has_param("format") ? req.get_param_value("format") : "";
        auto ret = TabletPortalBase::BuildRawDoc(tabletName, docStr, formatStr);
        if (ret.IsOK()) {
            res.set_content("success", "text/plain");
        } else {
            res.set_content(ret.ToString(), "text/plain");
        }
    };

    _httpServer.Get("/buildDoc", buildDocFunc);
    _httpServer.Post("/buildDoc", buildDocFunc);
}

void HttpTabletPortal::RegisterMethod_Stop()
{
    _httpServer.Get("/stop", [&](const httplib::Request& req, httplib::Response& res) { Stop(); });
    _httpServer.Post("/stop", [&](const httplib::Request& req, httplib::Response& res) { Stop(); });
}

} // namespace indexlibv2::framework
