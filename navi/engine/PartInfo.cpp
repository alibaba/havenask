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
#include "navi/engine/PartInfo.h"
#include "navi/util/ReadyBitMap.h"

namespace navi {

void PartInfo::init(NaviPartId partCount) {
    _ids.init(partCount);
    _ids.set();
}

void PartInfo::init(NaviPartId partCount, const PartInfoDef &def) {
    _ids.init(partCount);
    if (def.part_ids_size() == 0) {
        _ids.set();
    } else {
        for (const auto& id : def.part_ids()) {
            _ids.set(id, true);
        }
    }
}

void PartInfo::fillDef(PartInfoDef &def) const {
    def.set_part_count(_ids.size());
    def.clear_part_ids();
    for (size_t i = 0; i < _ids.size(); ++i) {
        if (_ids[i]) {
            def.add_part_ids(i);
        }
    }
}

ReadyBitMap *PartInfo::createReadyBitMap(autil::mem_pool::Pool *pool) const {
    auto *eof = ReadyBitMap::createReadyBitMap(pool, getFullPartCount());
    eof->setValid(false);
    for (auto partId : *this) {
        eof->setValid(partId, true);
    }
    return eof;
}

NaviPartId PartInfo::getUsedPartId(NaviPartId index) const {
    for (size_t i = 0; i < _ids.size(); ++i) {
        if (_ids[i] && (index-- == 0)) {
            return i;
        }
    }
    assert(false && "index must less than size");
    return -1;
}

NaviPartId PartInfo::getUsedPartIndex(NaviPartId partId) const {
    if (partId >= getFullPartCount()) {
        return -1;
    }
    NaviPartId index = 0;
    for (NaviPartId i = 0; i < partId; ++i) {
        if (_ids[i]) {
            ++index;
        }
    }
    if (_ids[partId]) {
        return index;
    } else {
        return -1;
    }
}

bool PartInfo::isUsed(NaviPartId partId) const {
    if (partId < _ids.size()) {
        return _ids[partId];
    }
    return false;
}

}
