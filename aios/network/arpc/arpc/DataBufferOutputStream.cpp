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
#include <stddef.h>
#include <cassert>

#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/arpc/arpc/DataBufferOutputStream.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/CommonMacros.h"

ARPC_BEGIN_NAMESPACE(arpc);
using namespace anet;
const int DataBufferOutputStream::BLOCK_SIZE;

DataBufferOutputStream::DataBufferOutputStream(DataBuffer *dataBuffer, int blockSize)
{
    assert(dataBuffer);
    _dataBuffer = dataBuffer;
    _blockSize = blockSize;
    _byteCount = 0;
}

DataBufferOutputStream::~DataBufferOutputStream()
{
    _dataBuffer = NULL;
    _byteCount = 0;
}

bool DataBufferOutputStream::Next(void **data, int *size)
{
    if (_dataBuffer->getFreeLen() == 0) {
        _dataBuffer->ensureFree(_blockSize);
    }

    *data = _dataBuffer->getFree();
    *size = _dataBuffer->getFreeLen();

    _dataBuffer->pourData(*size);
    _byteCount += *size;

    return true;
}

void DataBufferOutputStream::BackUp(int count)
{
    _dataBuffer->stripData(count);
    _byteCount -= count;
}

RPCint64 DataBufferOutputStream::ByteCount() const
{
    return _byteCount;
}

ARPC_END_NAMESPACE(arpc);
