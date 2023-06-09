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
#include "suez/sdk/SearchManager.h"

#include "autil/Log.h"
#include "iquan/common/catalog/CatalogInfo.h"

using namespace autil;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SearchManager);

SearchManager::SearchManager() {}

SearchManager::~SearchManager() {}

bool SearchManager::init(const SearchInitParam &initParam) { return true; }

autil::legacy::json::JsonMap SearchManager::getServiceInfo() const { return autil::legacy::json::JsonMap(); }

std::unique_ptr<iquan::CatalogInfo> SearchManager::getCatalogInfo() const { return nullptr; }

void SearchManager::updateStatus(bool status) { _innerUpdatingStatus.store(status); }

#define UPDATE_LAST_TIMESTAMP(var, timestamp)                                                                          \
    {                                                                                                                  \
        if (var.load() != timestamp) {                                                                                 \
            var.store(timestamp);                                                                                      \
            AUTIL_LOG(INFO, #var " updated to [%ld]", timestamp);                                                      \
        }                                                                                                              \
    }

void SearchManager::updateIssuedVersionTime(int64_t t) { UPDATE_LAST_TIMESTAMP(_lastIssuedVersionTimestamp, t); }

void SearchManager::updateTableVersionTime(int64_t t) { UPDATE_LAST_TIMESTAMP(_lastUpdatedTableVersionTimestamp, t); }

void SearchManager::updateVersionTime(int64_t t) { UPDATE_LAST_TIMESTAMP(_lastUpdatedVersionTimestamp, t); }

} // namespace suez
