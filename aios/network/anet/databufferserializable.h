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
/**
 * File name: databufferserializable.h
 * Author: lizhang
 * Create time: 2010-11-07 06:11:52
 * $Id$
 *
 * Description: ***add description here***
 *
 */

#ifndef ANET_DATABUFFERSERIALIZABLE_H_
#define ANET_DATABUFFERSERIALIZABLE_H_

#include <stdint.h>

#include "aios/network/anet/databuffer.h"

namespace anet {
class DataBufferSerializable {
public:
    DataBufferSerializable() {}
    virtual ~DataBufferSerializable() {}

    virtual bool serialize(DataBuffer *inputBuffer) const = 0;

    /**
     * deserialze from inputBuffer. if length is greater then 0, deserialze()
     * should consume length bytes from inputBuffer, Otherwise, the the data
     * in the inputBuffer either self-delimited or limited by the boundary of
     * inputBuffer
     */
    virtual bool deserialize(DataBuffer *inputBuffer, int length = 0) = 0;
    virtual void free() { delete this; }

    virtual int64_t getSpaceUsed() { return 0; };

    /**
     * @brief 获取序列化后所占用的内存大小
     */
    virtual size_t getSerializedSize() const { return 0; }
};

} /*end namespace anet*/
#endif /*ANET_DATABUFFERSERIALIZABLE_H_*/
