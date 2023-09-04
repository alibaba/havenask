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
#include "swift/admin/modules/DealErrorBrokerModule.h"

#include <algorithm>
#include <curl/curl.h>
#include <functional>
#include <memory>
#include <stddef.h>
#include <stdint.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/admin/WorkerTable.h"
#include "swift/common/Common.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/common/PathDefine.h"
#include "swift/config/AdminConfig.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/util/IpUtil.h"
#include "swift/util/TargetRecorder.h"

namespace swift {
namespace admin {

using namespace autil;
using namespace swift::common;
using namespace swift::util;
using namespace swift::protocol;

AUTIL_LOG_SETUP(swift, DealErrorBrokerModule);

const uint32_t HTTP_OK = 200;

DealErrorBrokerModule::DealErrorBrokerModule() : _workerTable(nullptr), _reporter(nullptr) {}

DealErrorBrokerModule::~DealErrorBrokerModule() {}

bool DealErrorBrokerModule::doInit() {
    _workerTable = _sysController->getWorkerTable();
    _reporter = _sysController->getMetricsReporter();
    return true;
}

bool DealErrorBrokerModule::doLoad() {
    if (!_adminConfig->getDealErrorBroker()) {
        AUTIL_LOG(INFO, "config not deal error brokers");
        return true;
    }
    _loopThread = LoopThread::createLoopThread(
        std::bind(&DealErrorBrokerModule::dealErrorBrokers, this), 20 * 1000 * 1000, "dealErrorBroker");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create deal error broker thread fail");
        return false;
    } else {
        AUTIL_LOG(INFO, "create deal error broker thread success, interval 20s");
        return true;
    }
}

bool DealErrorBrokerModule::doUnload() {
    if (_loopThread) {
        _loopThread->stop();
    }
    return true;
}

size_t writeFunction(void *ptr, size_t size, size_t nmemb, std::string *data) {
    data->append((char *)ptr, size * nmemb);
    return size * nmemb;
}

int64_t performCurlRequest(const std::string &url, const std::string &data, std::string &response) {
    int64_t resCode = CURLE_OK;
    CURL *curl = curl_easy_init();
    if (curl) {
        curl_slist *pList = NULL;
        pList = curl_slist_append(pList, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, pList);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFunction);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L); //忽略证书检查
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
        curl_easy_perform(curl);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &resCode);
        curl_easy_cleanup(curl);
        curl = NULL;
    }
    return resCode;
}

bool DealErrorBrokerModule::canDeal(const std::vector<std::string> &workers) {
    if (0 == workers.size()) {
        return false;
    }
    uint32_t count = _adminConfig->getErrorBrokerDealRatio() * _workerTable->getBrokerCount();
    count = std::max(count, (uint32_t)1);
    return workers.size() <= count;
}

void DealErrorBrokerModule::dealErrorBrokers() {
    std::vector<std::string> zombieWorkers;
    std::vector<std::string> timeoutWorkers;
    size_t errorCount = _workerTable->findErrorBrokers(zombieWorkers,
                                                       timeoutWorkers,
                                                       _adminConfig->getDeadBrokerTimeoutSec(),
                                                       _adminConfig->getZfsTimeout(),
                                                       _adminConfig->getCommitDelayThreshold());
    if (0 == errorCount) {
        AUTIL_LOG(INFO, "not find error brokers");
        return;
    }
    AUTIL_LOG(WARN,
              "find zombie brokers num[%ld], timeout brokers num[%ld], deal thresold[%f]",
              zombieWorkers.size(),
              timeoutWorkers.size(),
              _adminConfig->getErrorBrokerDealRatio());
    if (canDeal(zombieWorkers)) { // restart broker
        std::string roleName, address, ip;
        uint16_t port;
        if (PathDefine::parseRoleAddress(zombieWorkers[0], roleName, address) &&
            PathDefine::parseAddress(address, ip, port)) {
            const std::string &requestData = StringUtil::formatString("{\"ip\":\"%s\",\"submitter\":\"%s_%s\"}",
                                                                      ip.c_str(),
                                                                      _adminConfig->getServiceName().c_str(),
                                                                      IpUtil::getIp().c_str());
            std::string response;
            const std::string &url = _adminConfig->getReportZombieUrl();
            int64_t retCode = performCurlRequest(url, requestData, response);
            if (HTTP_OK != retCode) {
                AUTIL_LOG(WARN,
                          "report zombie[%s %s] failed, url[%s], data[%s], ret[%ld], response[%s]",
                          roleName.c_str(),
                          ip.c_str(),
                          url.c_str(),
                          requestData.c_str(),
                          retCode,
                          response.c_str());
            } else {
                if (_reporter) {
                    _reporter->incDealErrorBrokersQps(zombieWorkers[0], monitor::ERROR_TYPE_ZOMBIE);
                }
                AUTIL_LOG(INFO,
                          "report zombie[%s %s] success, url[%s], data[%s], ret[%ld], response[%s]",
                          roleName.c_str(),
                          ip.c_str(),
                          url.c_str(),
                          requestData.c_str(),
                          retCode,
                          response.c_str());
            }
        } else {
            AUTIL_LOG(WARN, "parse address[%s] failed, cannnot restart", zombieWorkers[0].c_str());
        }
    }
    if (canDeal(timeoutWorkers)) { // change slot
        std::string roleName, address;
        if (PathDefine::parseRoleAddress(timeoutWorkers[0], roleName, address)) {
            ChangeSlotRequest request;
            *request.add_rolenames() = roleName;
            ChangeSlotResponse response;
            _sysController->changeSlot(&request, &response);
            if (_reporter) {
                _reporter->incDealErrorBrokersQps(timeoutWorkers[0], monitor::ERROR_TYPE_TIMEOUT);
            }
            AUTIL_LOG(INFO,
                      "[%s %s] change slot, request[%s], response[%s]",
                      roleName.c_str(),
                      address.c_str(),
                      request.ShortDebugString().c_str(),
                      response.ShortDebugString().c_str());
        } else {
            AUTIL_LOG(ERROR, "parse Address[%s] fail, cannot cs", timeoutWorkers[0].c_str());
        }
    }
}

REGISTER_MODULE(DealErrorBrokerModule, "M|S", "L");
} // namespace admin
} // namespace swift
