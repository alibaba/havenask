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

#include <stdint.h>

#include "autil/Block.h"
#include "autil/DoubleLinkListNode.h"

namespace autil {

class BlockLinkListNode : public DoubleLinkListNode<Block *> {
public:
    void setKey(blockid_t id) { _data->_id = id; }
    blockid_t getKey() const { return _data->_id; }
    uint32_t getRefCount() const { return _data->getRefCount(); }
    uint32_t incRefCount() { return _data->incRefCount(); }
    uint32_t decRefCount() { return _data->decRefCount(); }
};

} // namespace autil
