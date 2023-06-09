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

namespace indexlib { namespace util {

struct BlockId {
    BlockId() : fileId(0), inFileIdx(0) {}
    BlockId(uint64_t fileId_, uint64_t inFileIdx_) : fileId(fileId_), inFileIdx(inFileIdx_) {}

    bool operator==(const BlockId& other) const { return fileId == other.fileId && inFileIdx == other.inFileIdx; }

    uint64_t fileId;
    uint64_t inFileIdx;
};
typedef BlockId blockid_t;

struct Block {
    Block(const blockid_t& id_, uint8_t* data_) : id(id_), data(data_) {}
    Block() : data(NULL) {}

    blockid_t id;
    uint8_t* data;
};
}} // namespace indexlib::util
