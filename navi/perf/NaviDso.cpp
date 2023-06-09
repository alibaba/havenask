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
#include "navi/perf/NaviDso.h"
#include <fcntl.h>
#include <libelf.h>

namespace navi {

NaviDso::NaviDso(const std::string &path, uint64_t base, uint64_t start, uint64_t end)
    : _path(path)
    , _base(base)
    , _start(start)
    , _end(end)
    , _fd(-1)
    , _elf(nullptr)
{
}

NaviDso::~NaviDso() {
    if (_elf) {
        elf_end(_elf);
    }
    if (_fd != -1) {
        close(_fd);
    }
}

bool NaviDso::init(const NaviLoggerPtr &logger) {
    _logger.object = this;
    _logger.logger = logger;
    _logger.addPrefix("file %s base 0x%lx start 0x%lx end 0x%lx", _path.c_str(),
                      _base, _start, _end);
    if (!loadElf()) {
        return false;
    }
    if (!readSymbolTable()) {
        return false;
    }
    readPltSymbols();
    std::sort(_symbolVec.begin(), _symbolVec.end(), compareSymbols);
    // std::cout << "init dso " << _path << " success" << std::endl;
    return true;
}

bool NaviDso::getVdsoRange(uint64_t &begin, uint64_t &end) {
    auto file = fopen("/proc/self/maps", "r");
    if (!file) {
        return false;
    }
    char buf[1024];
    bool found = false;
    while (fgets(buf, 1024, file)) {
        if (strstr(buf, "[vdso]")) {
            found = true;
            break;
        }
    }
    fclose(file);
    if (!found) {
        return false;
    }
    if (2 != sscanf(buf, "%lx-%lx", &begin, &end)) {
        return false;
    }
    return true;
}

bool NaviDso::loadElf() {
    if (_path == "[vdso]" || _path == "linux-vdso.so.1") {
        uint64_t begin = 0;
        uint64_t end = 0;
        if (!getVdsoRange(begin, end)) {
            NAVI_LOG(ERROR, "get vdso mem range failed");
            return false;
        }
        if (begin != _base) {
            NAVI_LOG(
                ERROR,
                "get vdso mem range failed, begin [%lx] and base not match",
                begin);
            return false;
        }
        _elf = elf_memory((char *)begin, end - begin);
    } else {
        _fd = open(_path.c_str(), O_RDONLY, 0);
        if (_fd < 0) {
            NAVI_LOG(INFO, "open file failed");
            return false;
        }
        _elf = elf_begin(_fd, ELF_C_READ_MMAP, nullptr);
    }
    if (!_elf) {
        NAVI_LOG(INFO, "open elf failed");
        return false;
    }
    if (!gelf_getehdr(_elf, &_ehdr)) {
        NAVI_LOG(INFO, "gelf_getehdr failed");
        return false;
    }
    loadSymbolTable(".symtab", _symTab);
    if (!loadSymbolTable(".dynsym", _dynSymTab)) {
        NAVI_LOG(INFO, "elf get section [%s] failed", ".dynsym");
        return false;
    }
    return true;
}

bool NaviDso::loadSymbolTable(const char *name,
                              ElfSymbolTable &elfSymTab)
{
    elfSymTab.sec = elf_section_by_name(_elf, &_ehdr, &elfSymTab.shdr, name,
                                        &elfSymTab.index);
    if (!elfSymTab.sec) {
        return false;
    }
    elfSymTab.syms = elf_getdata(elfSymTab.sec, nullptr);
    if (!elfSymTab.syms) {
        NAVI_LOG(INFO, "elf get sym section data failed");
        return false;
    }
    Elf_Scn *sec = elf_getscn(_elf, elfSymTab.shdr.sh_link);
    if (!sec) {
        NAVI_LOG(INFO, "elf get section [%d] failed", elfSymTab.shdr.sh_link);
        return false;
    }
    elfSymTab.symStrs = elf_getdata(sec, nullptr);
    if (!elfSymTab.symStrs) {
        NAVI_LOG(INFO, "elf get symbol string table failed");
        return false;
    }
    return true;
}

bool NaviDso::readSymbolTable() {
    GElf_Shdr shdr;
    Elf_Data *syms;
    Elf_Data *symStrs;
    if (_symTab.symStrs) {
        shdr = _symTab.shdr;
        syms = _symTab.syms;
        symStrs = _symTab.symStrs;
    } else {
        shdr = _dynSymTab.shdr;
        syms = _dynSymTab.syms;
        symStrs = _dynSymTab.symStrs;
    }
    if (symStrs->d_size == 0) {
        return true;
    }
    auto strTable = (const char *)symStrs->d_buf;
    uint64_t strTableSize = symStrs->d_size;
    uint32_t symsNum = shdr.sh_size / shdr.sh_entsize;
    GElf_Sym sym;
    memset(&sym, 0, sizeof(sym));
    _symbolVec.reserve(symsNum);
    for (uint32_t index = 0; index < symsNum; index++) {
        gelf_getsym(syms, index, &sym);
        if ((ELF64_ST_TYPE(sym.st_info) == STT_FUNC) &&
            (sym.st_shndx != SHN_UNDEF || sym.st_value != 0) &&
            sym.st_name < strTableSize)
        {
            DsoSymbol dsoSymbol;
            dsoSymbol.start = sym.st_value;
            dsoSymbol.size = sym.st_size;
            dsoSymbol.plt = false;
            dsoSymbol.end = sym.st_value + sym.st_size;
            dsoSymbol.nameOffset = sym.st_name;
            dsoSymbol.strTable = strTable;
            _symbolVec.push_back(dsoSymbol);
        }
    }
    return true;
}

bool NaviDso::readPltSymbols() {
    GElf_Shdr shdr_rel_plt;
    Elf_Scn *scn_plt_rel =
        elf_section_by_name(_elf, &_ehdr, &shdr_rel_plt, ".rela.plt", nullptr);
    if (!scn_plt_rel) {
        const char *relplt = ".rel.plt";
        scn_plt_rel =
            elf_section_by_name(_elf, &_ehdr, &shdr_rel_plt, relplt, nullptr);
        if (!scn_plt_rel) {
            NAVI_LOG(INFO, "get plt[%s] section failed", relplt);
            return false;
        }
    }
    const char *plt = ".plt";
    GElf_Shdr shdr_plt;
    if (!elf_section_by_name(_elf, &_ehdr, &shdr_plt, plt, nullptr)) {
        NAVI_LOG(INFO, "get plt[%s] section failed", plt);
        return false;
    }
    Elf_Data *relData = elf_getdata(scn_plt_rel, nullptr);
    if (!relData) {
        return false;
    }
    auto strTable = (const char *)_dynSymTab.symStrs->d_buf;
    uint32_t nr_rel_entries = shdr_rel_plt.sh_size / shdr_rel_plt.sh_entsize;
    uint64_t plt_addr = shdr_plt.sh_addr;
    int symidx;
    GElf_Sym sym;
    if (shdr_rel_plt.sh_type == SHT_RELA) {
        GElf_Rela pos_mem, *pos;
        for (size_t i = 0; i < nr_rel_entries; i++) {
            pos = gelf_getrela(relData, i, &pos_mem);
            symidx = GELF_R_SYM(pos->r_info);
            plt_addr += shdr_plt.sh_entsize;
            gelf_getsym(_dynSymTab.syms, symidx, &sym);
            DsoSymbol dsoSymbol;
            dsoSymbol.start = plt_addr;
            dsoSymbol.size = shdr_plt.sh_entsize;
            dsoSymbol.plt = true;
            dsoSymbol.end = dsoSymbol.start + shdr_plt.sh_entsize;
            dsoSymbol.nameOffset = sym.st_name;
            dsoSymbol.strTable = strTable;
            _symbolVec.push_back(dsoSymbol);
        }
    } else if (shdr_rel_plt.sh_type == SHT_REL) {
        GElf_Rel pos_mem, *pos;
        for (size_t i = 0; i < nr_rel_entries; i++) {
            pos = gelf_getrel(relData, i, &pos_mem);
            symidx = GELF_R_SYM(pos->r_info);
            plt_addr += shdr_plt.sh_entsize;
            gelf_getsym(_dynSymTab.syms, symidx, &sym);
            DsoSymbol dsoSymbol;
            dsoSymbol.start = plt_addr;
            dsoSymbol.size = shdr_plt.sh_entsize;
            dsoSymbol.plt = true;
            dsoSymbol.end = dsoSymbol.start + shdr_plt.sh_entsize;
            dsoSymbol.nameOffset = sym.st_name;
            dsoSymbol.strTable = strTable;
            _symbolVec.push_back(dsoSymbol);
        }
    }
    return true;
}

Elf_Scn *NaviDso::elf_section_by_name(Elf *elf,
                                      GElf_Ehdr *ep,
                                      GElf_Shdr *shp,
                                      const char *name,
                                      size_t *idx)
{
    Elf_Scn *sec = nullptr;
    size_t cnt = 1;
    if (!elf_rawdata(elf_getscn(elf, ep->e_shstrndx), nullptr)) {
        return nullptr;
    }
    while ((sec = elf_nextscn(elf, sec)) != nullptr) {
        char *str;
        gelf_getshdr(sec, shp);
        str = elf_strptr(elf, ep->e_shstrndx, shp->sh_name);
        if (str && !strcmp(name, str)) {
            if (idx) {
                *idx = cnt;
            }
            return sec;
        }
        ++cnt;
    }
    return NULL;
}

bool NaviDso::compareDsos(const NaviDso *left, const NaviDso *right) {
    return left->_start + left->_base > right->_start + right->_base;
}

bool NaviDso::compareDsoAndAddr(uint64_t addr, const NaviDso *dso) {
    return addr >= dso->_start + dso->_base;
}

bool NaviDso::compareSymbols(const DsoSymbol &left, const DsoSymbol &right) {
    return left.start > right.start;
}

bool NaviDso::compareSymbolAndAddr(uint64_t addr, const DsoSymbol &symbol) {
    return addr >= symbol.start;
}

const char *NaviDso::findSymbol(uint64_t addr, bool &plt) const {
    if (addr < _start + _base || addr > _end + _base) {
        return nullptr;
    }
    uint64_t offset = addr - _base;
    auto it = std::upper_bound(_symbolVec.begin(), _symbolVec.end(), offset,
                               compareSymbolAndAddr);
    if (_symbolVec.end() == it) {
        return nullptr;
    }
    if ((offset >= it->start && offset < it->end) ||
        (it->size == 0 && offset == it->start))
    {
        plt = it->plt;
        return it->strTable + it->nameOffset;
    } else {
        return nullptr;
    }
}

const char *NaviDso::getPath() const {
    return _path.c_str();
}

uint64_t NaviDso::getSymbolCount() const {
    return _symbolVec.size();
}

}

