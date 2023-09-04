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
#include "autil/LzmaCompressor.h"

#include <lzma.h>

#include "autil/Log.h"

using namespace std;

namespace autil {

AUTIL_DECLARE_AND_SETUP_LOGGER(autil, LzmaCompressor);

LzmaCompressor::LzmaCompressor(uint32_t preset) : _preset(preset) {}

LzmaCompressor::~LzmaCompressor() {}

bool init_decoder(lzma_stream *strm) {
    lzma_ret ret = lzma_stream_decoder(strm, UINT64_MAX, LZMA_CONCATENATED);
    if (ret == LZMA_OK) {
        return true;
    }
    string msg;
    switch (ret) {
    case LZMA_MEM_ERROR:
        msg = "Memory allocation failed";
        break;
    case LZMA_OPTIONS_ERROR:
        msg = "Unsupported decompressor flags";
        break;
    default:
        msg = "Unknown error, possibly a bug";
        break;
    }

    AUTIL_LOG(ERROR, "Error initializing the decoder: %s (error code %u)", msg.c_str(), ret);
    return false;
}

bool init_encoder(lzma_stream *strm, uint32_t preset) {
    lzma_ret ret = lzma_easy_encoder(strm, preset, LZMA_CHECK_CRC64);

    if (ret == LZMA_OK) {
        return true;
    }

    string msg;
    switch (ret) {
    case LZMA_MEM_ERROR:
        msg = "Memory allocation failed";
        break;
    case LZMA_OPTIONS_ERROR:
        msg = "Specified preset is not supported";
        break;
    case LZMA_UNSUPPORTED_CHECK:
        msg = "Specified integrity check is not supported";
        break;
    default:
        msg = "Unknown error, possibly a bug";
        break;
    }

    AUTIL_LOG(ERROR, "Error initializing the encoder: %s (error code %u)", msg.c_str(), ret);
    return false;
}

bool transfer(lzma_stream *strm, const std::string &input, std::string &output) {
    lzma_action action = LZMA_RUN;
    uint8_t outbuf[BUFSIZ];
    strm->next_in = (const uint8_t *)input.c_str();
    strm->avail_in = input.size();
    strm->next_out = outbuf;
    strm->avail_out = sizeof(outbuf);

    while (true) {
        if (strm->avail_in == 0) {
            action = LZMA_FINISH;
        }
        lzma_ret ret = lzma_code(strm, action);
        if (strm->avail_out == 0 || ret == LZMA_STREAM_END) {
            size_t write_size = sizeof(outbuf) - strm->avail_out;
            output.append((const char *)outbuf, write_size);

            strm->next_out = outbuf;
            strm->avail_out = sizeof(outbuf);
        }

        if (ret != LZMA_OK) {
            if (ret == LZMA_STREAM_END) {
                return true;
            }

            string msg;
            switch (ret) {
            case LZMA_MEM_ERROR:
                msg = "Memory allocation failed";
                break;
            case LZMA_DATA_ERROR:
                msg = "File size limits exceeded or Compressed file is corrupt";
                break;
            case LZMA_FORMAT_ERROR:
                msg = "The input is not in the lzma format";
                break;
            case LZMA_OPTIONS_ERROR:
                msg = "Unsupported compression options";
                break;
            case LZMA_BUF_ERROR:
                msg = "Compressed file is truncated or "
                      "otherwise corrupt";
                break;
            default:
                msg = "Unknown error, possibly a bug";
                break;
            }

            AUTIL_LOG(ERROR, "Transfer error: %s (error code %u)", msg.c_str(), ret);
            return false;
        }
    }

    assert(false);
    AUTIL_LOG(ERROR, "control flow should not goto here");
    return false;
}

bool LzmaCompressor::compress(const std::string &input, std::string &output) {
    lzma_stream strm = LZMA_STREAM_INIT;
    bool success = init_encoder(&strm, _preset);
    if (success) {
        success = transfer(&strm, input, output);
    }
    lzma_end(&strm);
    return success;
}

bool LzmaCompressor::decompress(const std::string &input, std::string &output) {
    lzma_stream strm = LZMA_STREAM_INIT;
    bool success = init_decoder(&strm);
    if (success) {
        success = transfer(&strm, input, output);
    }
    lzma_end(&strm);
    return success;
}

} // namespace autil
