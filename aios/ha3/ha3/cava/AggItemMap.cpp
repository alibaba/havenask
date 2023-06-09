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
#include "ha3/cava/AggItemMap.h"

#include "cava/runtime/Allocator.h"

class CavaCtx;

namespace ha3 {
class MChar;
}  // namespace ha3

using namespace std;

namespace unsafe {

#define DEFINE_AGGITEMMAP_CLASS(name, type)                             \
    name##AggItemMap::name##AggItemMap() {                              \
    }                                                                   \
    name##AggItemMap *name##AggItemMap::create(CavaCtx *ctx) {          \
        return cava::AllocUtil::createNativeObject<name##AggItemMap>(ctx); \
    }

DEFINE_AGGITEMMAP_CLASS(Byte, cava::byte);
DEFINE_AGGITEMMAP_CLASS(Short, short);
DEFINE_AGGITEMMAP_CLASS(Int, int);
DEFINE_AGGITEMMAP_CLASS(Long, long);
DEFINE_AGGITEMMAP_CLASS(UByte, ubyte);
DEFINE_AGGITEMMAP_CLASS(UShort, ushort);
DEFINE_AGGITEMMAP_CLASS(UInt, uint);
DEFINE_AGGITEMMAP_CLASS(ULong, ulong);
DEFINE_AGGITEMMAP_CLASS(Float, float);
DEFINE_AGGITEMMAP_CLASS(Double, double);
DEFINE_AGGITEMMAP_CLASS(MChar, ha3::MChar*);

}
