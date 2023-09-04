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

#include "navi/log/NaviLogger.h"
#include "navi/perf/NaviDso.h"
#include <link.h>

namespace navi {

class PerfSymbolTableDef;

class NaviSymbolTable
{
public:
    NaviSymbolTable();
    ~NaviSymbolTable();
private:
    NaviSymbolTable(const NaviSymbolTable &);
    NaviSymbolTable &operator=(const NaviSymbolTable &);
public:
    bool init(const NaviLoggerPtr &logger);
    void resolveSymbol(const std::unordered_map<uint64_t, uint32_t> &addrMap,
                       PerfSymbolTableDef *perfSymbolTableDef) const;
    std::string resolveAddr(uint64_t ip) const;
public:
    static std::string parseSymbol(const char *symbol);
private:
    const char *findSymbol(uint64_t addr, const char *&dso, bool &plt) const;
    bool initElfVersion() const;
    bool initLoadLibs();
    void addDso(NaviDso *dso);
    std::string getBinaryName() const;
    bool searchAddrInMaps(size_t addr, std::string &filePath) const;
    const NaviLoggerPtr &getLogger() const;
private:
    static int dlCallback(struct dl_phdr_info *info, size_t size, void *data);
private:
    DECLARE_LOGGER();
    std::vector<NaviDso *> _dsoVec;
    uint64_t _totalSymbolCount;
};

NAVI_TYPEDEF_PTR(NaviSymbolTable);

}
