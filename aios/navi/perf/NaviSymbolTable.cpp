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
#include "navi/perf/NaviSymbolTable.h"
#include "navi/proto/GraphVis.pb.h"
#include <algorithm>
#include <autil/StringUtil.h>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <fstream>
#include <libelf.h>
#include <limits.h>
#include <stdlib.h>

namespace navi {

NaviSymbolTable::NaviSymbolTable()
    : _totalSymbolCount(0)
{
}

NaviSymbolTable::~NaviSymbolTable() {
    for (auto dso : _dsoVec) {
        delete dso;
    }
}

bool NaviSymbolTable::init(const NaviLoggerPtr &logger) {
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("navi symbol table");
    if (!initElfVersion()) {
        return false;
    }
    if (!initLoadLibs()) {
        return false;
    }
    NAVI_LOG(
        INFO,
        "init navi symbol table success, lib count: %lu, symbol count: %lu",
        _dsoVec.size(), _totalSymbolCount);
    return true;
}

bool NaviSymbolTable::initElfVersion() const {
    if (elf_version(EV_CURRENT) == EV_NONE) {
        return false;
    }
    return true;
}

bool NaviSymbolTable::searchAddrInMaps(size_t addr, std::string &filePath) const {
    auto file = fopen("/proc/self/maps", "r");
    if (!file) {
        return false;
    }
    bool ret = false;
    while (true) {
        char buffer[4096];
        char prot[5];
        char execName[4096];
        if (!fgets(buffer, sizeof(buffer), file)) {
            break;
        }
        strcpy(execName, "");
        uint64_t start, end, pgoff, maj, min, inode;
        /* 00400000-0040c000 r-xp 00000000 fd:01 41038  /bin/cat */
        auto n = sscanf(
            buffer, "%lx-%lx %s %lx %lx:%lx %lu %[^\n]\n", &start, &end, prot, &pgoff, &maj, &min, &inode, execName);
        if (n < 8) {
            continue;
        }
        if (addr == start) {
            filePath = execName;
            ret = true;
        }
    }
    fclose(file);
    return ret;
}

std::string NaviSymbolTable::getBinaryName() const {
    char buff[PATH_MAX];
    ssize_t len = ::readlink("/proc/self/exe", buff, sizeof(buff)-1);
    if (len != -1) {
        buff[len] = '\0';
        return std::string(buff);
    } else {
        return std::string();
    }
}

const NaviLoggerPtr &NaviSymbolTable::getLogger() const {
    return _logger.logger;
}

int NaviSymbolTable::dlCallback(struct dl_phdr_info *info, size_t size, void *data) {
    NaviSymbolTable *symbolTable = (NaviSymbolTable *)data;
    std::string filePath;
    if (info->dlpi_name) {
        filePath = info->dlpi_name;
    }
    auto base = info->dlpi_addr;
    if (filePath.empty()) {
        if (0 == base) {
            filePath = symbolTable->getBinaryName();
        } else if (!symbolTable->searchAddrInMaps(base, filePath)) {
            filePath = "[vdso]";
        }
    }
    auto phnum = info->dlpi_phnum;
    uint64_t minAddr = std::numeric_limits<uint64_t>::max();
    uint64_t maxAddr = 0;
    for (size_t i = 0; i < phnum; i++) {
        const auto &phdr = info->dlpi_phdr[i];
        auto start = phdr.p_vaddr;
        auto end = start + phdr.p_memsz;
        minAddr = std::min(minAddr, start);
        maxAddr = std::max(maxAddr, end);
    }
    auto dso = new NaviDso(filePath, base, minAddr, maxAddr);
    if (!dso->init(symbolTable->getLogger())) {
        delete dso;
        return 0;
    }
    symbolTable->addDso(dso);
    return 0;
}

void NaviSymbolTable::addDso(NaviDso *dso) {
    _totalSymbolCount += dso->getSymbolCount();
    _dsoVec.push_back(dso);
}

bool NaviSymbolTable::initLoadLibs() {
    dl_iterate_phdr(dlCallback, this);
    std::sort(_dsoVec.begin(), _dsoVec.end(), NaviDso::compareDsos);
    NAVI_LOG(INFO, "dso count: %lu", _dsoVec.size());
    return true;
}

const char *NaviSymbolTable::findSymbol(uint64_t addr, const char *&dso,
                                        bool &plt) const
{
    auto it = std::upper_bound(_dsoVec.begin(), _dsoVec.end(), addr,
                               NaviDso::compareDsoAndAddr);
    if (_dsoVec.end() == it) {
        return nullptr;
    }
    auto naviDso = *it;
    auto ret = naviDso->findSymbol(addr, plt);
    if (ret) {
        dso = naviDso->getPath();
    }
    return ret;
}

void NaviSymbolTable::resolveSymbol(
    const std::unordered_map<uint64_t, uint32_t> &addrMap,
    PerfSymbolTableDef *perfSymbolTableDef) const
{
    int32_t symbolCount = addrMap.size();
    auto symbolVecDef = perfSymbolTableDef->mutable_symbols();
    symbolVecDef->Reserve(symbolCount);
    for (size_t i = 0; i < symbolCount; i++) {
        symbolVecDef->Add();
    }
    for (const auto &pair : addrMap) {
        auto ip = pair.first;
        auto symbolStr = resolveAddr(ip);
        auto symbolDef = symbolVecDef->Mutable(pair.second);
        symbolDef->set_ip(ip);
        symbolDef->set_symbol(std::move(symbolStr));
    }
}

std::string NaviSymbolTable::resolveAddr(uint64_t ip) const {
    const char *dso = nullptr;
    bool plt = false;
    auto symbol = findSymbol(ip, dso, plt);
    std::string symbolStr;
    if (symbol) {
        symbolStr = parseSymbol(symbol);
        if (plt) {
            symbolStr += "@plt";
        }
    } else {
        char buffer[128];
        autil::StringUtil::uint64ToHexStr(ip, buffer, sizeof(buffer));
        symbolStr = std::string("0x") + buffer;
    }
    return symbolStr;
}

std::string NaviSymbolTable::parseSymbol(const char *symbol) {
    int status;
    char *s = abi::__cxa_demangle(symbol, NULL, 0, &status);
    if (status != 0) {
        return std::string(symbol);
    }
    std::string ret(s);
    free(s);
    return ret;
}

}
