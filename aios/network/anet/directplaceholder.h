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
#ifndef ANET_DIRECTPLACEHOLDER_H_
#define ANET_DIRECTPLACEHOLDER_H_

#include <cstdint>

namespace anet {

class DirectPlaceholder {
public:
    DirectPlaceholder() : _addr(nullptr), _len(0U) {}
    DirectPlaceholder(char *addr, uint32_t len) : _addr(addr), _len(len) {}
    char *getAddr() const { return _addr; }
    uint32_t getLen() const { return _len; }

private:
    char *_addr;
    uint32_t _len;
};

} // namespace anet

#endif // ANET_DIRECTPLACEHOLDER_H_
