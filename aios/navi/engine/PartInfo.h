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

#include "navi/common.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/util/BitMap.h"

namespace navi {

class ReadyBitMap;

class PartInfo
{
    struct Iterator {
        Iterator(const BitMap *bitMap_, size_t size_, bool end = false) : bitMap(bitMap_), size(size_) {
            curPos = end ? size : 0;
            moveToNext();
        }
        bool moveToNext() {
            while (curPos < size) {
                if ((*bitMap)[curPos]) { return true;}
                ++curPos;
            }
            return false;
        }
        Iterator &operator++() {
            if (++curPos < size) { moveToNext();}
            return *this;
        }
        size_t operator*() const { return curPos; }
        friend bool operator==(const Iterator &a, const Iterator &b) {
            return a.bitMap == b.bitMap && a.curPos == b.curPos;
        };
        friend bool operator!=(const Iterator &a, const Iterator &b) { return !(a == b); };
    private:
        const BitMap *bitMap;
        size_t size;
        size_t curPos {0};
    };

public:
    void init(NaviPartId partCount, const PartInfoDef &def);
    void init(NaviPartId partCount);
    void fillDef(PartInfoDef &def) const;
    NaviPartId getFullPartCount() const { return _ids.size(); }
    NaviPartId getUsedPartCount() const { return _ids.count(); }
    NaviPartId getUsedPartId(NaviPartId index) const;
    NaviPartId getUsedPartIndex(NaviPartId partId) const;
    bool isUsed(NaviPartId partId) const;
    ReadyBitMap *createReadyBitMap() const;
    const std::string ShortDebugString() const { return _ids.toString(); }

public:
    Iterator begin() const { return Iterator(&_ids, _ids.size()); }
    Iterator end() const { return Iterator(&_ids, _ids.size(), true); }

private:
    BitMap _ids;
};

} // namespace navi
