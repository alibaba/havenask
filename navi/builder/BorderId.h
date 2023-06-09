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
#ifndef NAVI_BORDERID_H
#define NAVI_BORDERID_H

#include "navi/common.h"
#include "autil/legacy/jsonizable.h"

namespace navi {

class BorderDef;
class BorderIdDef;

struct BorderId {
    BorderId(IoType ioType_, GraphId id_, GraphId peer_);
    BorderId(GraphId id_, const BorderDef &borderDef);
    BorderId(const BorderIdDef &borderIdDef);
public:
    bool operator==(const BorderId &other) const;
    friend std::ostream &operator<<(std::ostream &os, const BorderId &borderId);
public:
    IoType ioType;
    GraphId id;
    GraphId peer;
};

struct BorderIdJsonize : public autil::legacy::Jsonizable {
public:
    BorderIdJsonize(const BorderId &borderId_);
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    BorderId borderId;
};

class BorderIdHash {
public:
    size_t operator()(const BorderId &border) const;
};

}

#endif //NAVI_BORDERID_H
