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
#include "autil/ZlibCompressor.h"

#include <cassert>
#include <sstream>
#include <zconf.h>

using namespace std;

namespace autil {

ZlibCompressor::ZlibCompressor(const int type) : _compressType(type) {}

ZlibCompressor::ZlibCompressor(const ZlibCompressor &src) : _compressType(src._compressType) {}

ZlibCompressor::~ZlibCompressor() {}

bool ZlibCompressor::compress() {
    if (_bufferIn.getDataLen() == 0) {
        return false;
    }
    if (_bufferOut.getDataLen() == 0) {
        _bufferOut.reserve(DEFAULT_BUFFER_SIZE);
    }

    int ret, flush;
    uint32_t have;
    z_stream strm;

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = deflateInit(&strm, _compressType);
    if (ret != Z_OK) {
        return false;
    }

    strm.avail_in = _bufferIn.getDataLen();
    strm.next_in = (Bytef *)_bufferIn.getBuffer();
    flush = Z_FINISH;

    do {
        uint32_t remain = _bufferOut.remaining();
        strm.avail_out = remain;
        strm.next_out = (Bytef *)_bufferOut.getPtr();
        ret = deflate(&strm, flush);
        if (ret == Z_STREAM_ERROR) {
            return false;
        }

        have = remain - strm.avail_out;
        _bufferOut.movePtr(have);
        if (strm.avail_out == 0) {
            _bufferOut.reserve((_bufferOut.getTotalSize() << 1) - _bufferOut.getDataLen());
        }
    } while (strm.avail_out == 0);

    assert(strm.avail_in == 0);
    (void)deflateEnd(&strm);

    return true;
}

bool ZlibCompressor::decompress() {
    int ret;
    unsigned have;
    z_stream strm;

    if (_bufferOut.getDataLen() == 0) {
        _bufferOut.reserve(DEFAULT_BUFFER_SIZE);
    }

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = inflateInit(&strm);
    if (ret != Z_OK) {
        return false;
    }

    strm.avail_in = _bufferIn.getDataLen();
    strm.next_in = (Bytef *)_bufferIn.getBuffer();

    do {
        uint32_t remain = _bufferOut.remaining();
        strm.avail_out = remain;
        strm.next_out = (Bytef *)_bufferOut.getPtr();

        ret = inflate(&strm, Z_NO_FLUSH);
        switch (ret) {
        case Z_STREAM_ERROR:
        case Z_NEED_DICT:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
            (void)inflateEnd(&strm);
            return false;
        }
        have = remain - strm.avail_out;
        _bufferOut.movePtr(have);
        if (have == remain) {
            _bufferOut.reserve((_bufferOut.getTotalSize() << 1) - _bufferOut.getDataLen());
        }
    } while (strm.avail_out == 0);

    (void)inflateEnd(&strm);
    return true;
}

void ZlibCompressor::reset() {
    _bufferIn.reset();
    _bufferOut.reset();
}

bool ZlibCompressor::compress(const string &input, string &output) {
    if (input.empty()) {
        output = input;
        return true;
    }

    addDataToBufferIn(input);
    if (!compress()) {
        return false;
    }

    output.clear();
    output.assign(_bufferOut.getBuffer(), _bufferOut.getDataLen());
    return true;
}

bool ZlibCompressor::decompress(const string &input, string &output) {
    if (input.empty()) {
        output = input;
        return true;
    }

    addDataToBufferIn(input);
    if (!decompress()) {
        return false;
    }

    output.clear();
    output.assign(_bufferOut.getBuffer(), _bufferOut.getDataLen());
    return true;
}

} // namespace autil
