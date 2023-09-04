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
#ifndef ARPC_DATA_BUFFER_OUTPUT_STREAM_H
#define ARPC_DATA_BUFFER_OUTPUT_STREAM_H
#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "google/protobuf/io/zero_copy_stream.h"

ARPC_BEGIN_NAMESPACE(arpc)

class DataBufferOutputStream : public RPCZeroCopyOutputStream {
public:
    DataBufferOutputStream(anet::DataBuffer *dataBuffer, int blockSize = BLOCK_SIZE);
    virtual ~DataBufferOutputStream();

public:
    virtual bool Next(void **data, int *size);
    // it only can be called after Next
    // count must less than size which returned by last Next
    virtual void BackUp(int count);

    virtual RPCint64 ByteCount() const;

private:
    anet::DataBuffer *_dataBuffer;
    int _blockSize;
    RPCint64 _byteCount;
    // using value of kDefaultBlockSize in protocolbuffer 2.3.0
    static const int BLOCK_SIZE = 8192;
};

ARPC_END_NAMESPACE(arpc)

#endif
