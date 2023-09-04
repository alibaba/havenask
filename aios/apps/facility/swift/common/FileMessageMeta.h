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

namespace swift {
namespace common {

class FileMessageMeta {
public:
    FileMessageMeta()
        : timestamp(0), msgLenHighPart(0), endPos(0), isMerged(false), isCompress(false), maskPayload(0), payload(0) {}

    FileMessageMeta(int64_t timestamp,
                    uint64_t endPos,
                    uint16_t payload,
                    uint8_t maskPayload,
                    bool compress = false,
                    int64_t msgLenHighPart = 0,
                    bool merged = false) {
        this->maskPayload = maskPayload;
        this->payload = payload;
        this->timestamp = timestamp;
        this->endPos = endPos;
        this->isCompress = compress;
        this->msgLenHighPart = msgLenHighPart;
        this->isMerged = merged;
    }

public:
    void toBuf(char *buf) {
        uint8_t *p = (uint8_t *)buf;
        for (int i = 0; i < 6; ++i) {
            p[i] = (timestamp >> (8 * i)) & 255;
        }
        p[6] = (timestamp >> 48) & 0xf;
        p[6] |= (0xf & msgLenHighPart) << 4;
        p[7] = (msgLenHighPart >> 4) & 0xff;
        for (int i = 0; i < 5; ++i) {
            p[i + 8] = (endPos >> (8 * i)) & 255;
        }
        if (isMerged) {
            p[12] |= 0x40;
        }
        if (isCompress) {
            p[12] |= 0x80;
        }
        p[13] = maskPayload;
        for (int i = 0; i < 2; ++i) {
            p[i + 14] = (payload >> (8 * i)) & 255;
        }
    }

    void fromBuf(const char *buf) {
        timestamp = 0;
        endPos = 0;
        payload = 0;
        const uint8_t *p = (const uint8_t *)buf;
        for (int i = 0; i < 6; ++i) {
            timestamp |= uint64_t(p[i]) << (8 * i);
        }
        timestamp |= uint64_t(p[6] & 0xf) << 48;
        msgLenHighPart = p[6] >> 4;
        msgLenHighPart |= uint64_t(p[7]) << 4;

        for (int i = 0; i < 4; ++i) {
            endPos |= uint64_t(p[i + 8]) << (8 * i);
        }
        endPos |= uint64_t(p[12] & 0x3f) << 32;
        isMerged = p[12] & 0x40;
        isCompress = p[12] & 0x80;
        maskPayload = p[13];
        for (int i = 0; i < 2; ++i) {
            payload |= uint16_t(p[i + 14]) << (8 * i);
        }
    }

    bool operator==(const FileMessageMeta &meta) {
        return maskPayload == meta.maskPayload && payload == meta.payload && timestamp == meta.timestamp &&
               endPos == meta.endPos && isCompress == meta.isCompress && isMerged == meta.isMerged;
    }

private:
    void toBufOld(char *buf) {
        uint8_t *p = (uint8_t *)buf;
        for (int i = 0; i < 8; ++i) {
            p[i] = (timestamp >> (8 * i)) & 255;
        }
        for (int i = 0; i < 5; ++i) {
            p[i + 8] = (endPos >> (8 * i)) & 255;
        }
        if (isCompress) {
            p[12] |= 0x80;
        }
        p[13] = maskPayload;
        for (int i = 0; i < 2; ++i) {
            p[i + 14] = (payload >> (8 * i)) & 255;
        }
    }

public:
    uint64_t timestamp      : 52; // 4bit can be used in the feature
    uint64_t msgLenHighPart : 12; // len > 1M part
    uint64_t endPos         : 38; // file offset, in memory msg: offset:25 msg_count:10
    bool isMerged           : 1;
    bool isCompress         : 1;
    uint64_t maskPayload    : 8;
    uint64_t payload        : 16;
};

const int64_t FILE_MESSAGE_META_SIZE = sizeof(FileMessageMeta);

} // namespace common
} // namespace swift
