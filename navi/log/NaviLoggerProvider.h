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
#ifndef NAVI_NAVILOGGERPROVIDER_H
#define NAVI_NAVILOGGERPROVIDER_H

#include "navi/common.h"
#include <vector>

namespace navi {

class NaviLogManager;
class NaviLogger;
class NaviLoggerScope;
class NaviObjectLogger;

class NaviLoggerProvider
{
public:
    NaviLoggerProvider(const std::string &logLevel = "ERROR",
                       const std::string &prefix = "");
    ~NaviLoggerProvider();
private:
    NaviLoggerProvider(const NaviLoggerProvider &);
    NaviLoggerProvider &operator=(const NaviLoggerProvider &);
public:
    std::string getErrorMessage() const;
    std::vector<std::string> getTrace(
            const std::string &filter = "",
            const std::string &formatPattern = "%%m") const;
    void setFormatPattern(const std::string &pattern);
private:
    std::shared_ptr<NaviLogManager> _logManager;
    NaviObjectLogger *_logger;
    NaviLoggerScope *_scope;
};

}

#endif //NAVI_NAVILOGGERPROVIDER_H
