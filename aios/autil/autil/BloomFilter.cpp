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
#include "autil/BloomFilter.h"

#include <assert.h>
#include <netinet/in.h>
#include <string.h>
#include <zconf.h>
#include <zlib.h>
#include <iostream>
#include <algorithm>

#include "autil/HashFunction.h"
#include "autil/Log.h"
#include "autil/legacy/base64.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, BloomFilter);

const uint8_t BloomFilter::bit_mask[8] = {
                                     0x01,
                                     0x02,
                                     0x04,
                                     0x08,
                                     0x10,
                                     0x20,
                                     0x40,
                                     0x80
                                   };

BloomFilter::BloomFilter(uint32_t bits_size, uint32_t hash_num) {
    _elementNum = 0;
    _arraryBits = NULL;
    _hashFuncNum = (hash_num <= 10 ? hash_num : 10);
    if (0 != bits_size && 0 != hash_num) {
        _bits = bits_size;
        _bitsBufferSize = ((_bits >> 3) + ((_bits % 8) ? 1 : 0));
        _arraryBits = new uint8_t[_bitsBufferSize];
        assert(_arraryBits);
        memset(_arraryBits, 0x0, _bitsBufferSize);
    }
    RegisterHashFunctions();
}

BloomFilter::~BloomFilter() {
    if (_arraryBits != NULL) {
        delete []_arraryBits;
    }
}

void BloomFilter::RegisterHashFunctions() {
    _hashFunctions.push_back(storeRsHash);
    _hashFunctions.push_back(storeJsHash);
    _hashFunctions.push_back(storePjwHash);
    _hashFunctions.push_back(storeBkdrHash);
    _hashFunctions.push_back(storeSdbmHash);
    _hashFunctions.push_back(storeDjbHash);
    _hashFunctions.push_back(storeDekHash);
    _hashFunctions.push_back(storeBpHash);
    _hashFunctions.push_back(storeFnvHash);
    _hashFunctions.push_back(storeApHash);
    return;
}

void BloomFilter::Insert(const char* element, uint32_t len) {
    uint32_t hash = 0;
    uint32_t offset = 0;
    uint32_t byte_offset = 0;
    uint32_t bit_offset = 0;
    for (uint32_t i = 0; i < _hashFuncNum; i++) {
        hash = _hashFunctions[i](element, len);
        offset = hash % _bits;
        byte_offset = offset >> 3 ;
        bit_offset = offset % 8;
        SetBit(byte_offset, bit_offset);
    }
    _elementNum++;
    return;
}

bool BloomFilter::Contains(const char* element, uint32_t len) {
    uint32_t hash = 0;
    uint32_t offset = 0;
    uint32_t byte_offset = 0;
    uint32_t bit_offset = 0;
    for (uint32_t i = 0; i < _hashFuncNum; i++) {
        hash = _hashFunctions[i](element, len);
        offset = hash % _bits;
        byte_offset = offset >> 3;
        bit_offset = offset % 8;
        if (!TestBit(byte_offset, bit_offset)) {
            return false;
        }
    }
    return true;
}

bool BloomFilter::StreamToString(std::string& filterStr) const {
    char headerInfo[13] = {0};
    uint32_t size = 13;
    GenerateHeaderInfo(headerInfo, size);
    std::string headerStr(headerInfo, size);
    istringstream decodedHeader(headerStr);
    ostringstream encodedHeader;
    legacy::Base64Encoding(decodedHeader, encodedHeader);
    std::string filterHead = encodedHeader.str();

    //compress the filter bits array
    uLongf predict_size = compressBound(_bitsBufferSize);
    char* compress_buffer = new char[predict_size];
    compress((Bytef *)compress_buffer, &predict_size, ( const Bytef *)_arraryBits, _bitsBufferSize);
    //encode the bits array
    std::string bufferStr(compress_buffer, predict_size);
    istringstream decodedBuffer(bufferStr);
    ostringstream encodedBuffer;
    legacy::Base64Encoding(decodedBuffer, encodedBuffer);
    std::string filterBody = encodedBuffer.str();
    filterStr = filterHead + filterBody;
    delete []compress_buffer;
    return true;
}

bool BloomFilter::DecodeFromString(std::string& filterStr) {

    istringstream encodedfilter(filterStr);
    ostringstream decodedfilter;
    legacy::Base64Decoding(encodedfilter, decodedfilter);
    std::string decoded_filterStr = decodedfilter.str();
    const char* headInfo = decoded_filterStr.c_str();
    const uint32_t* pbits_num = reinterpret_cast<const uint32_t*>(headInfo);
    _bits = ntohl(*pbits_num);
    _bitsBufferSize = ((_bits >> 3) + ((_bits % 8) ? 1 : 0));
    const uint32_t* phash_num = reinterpret_cast<const uint32_t*>(headInfo + 4);
    _hashFuncNum = ntohl(*phash_num);
    const uint32_t* pelement_num = reinterpret_cast<const uint32_t*>(headInfo + 8);
    _elementNum = ntohl(*pelement_num);
    if (_arraryBits != NULL) {
        delete []_arraryBits;
    }
    _arraryBits = new uint8_t[_bitsBufferSize];
    uLongf uncompress_buffer_size = _bitsBufferSize + 1;
    char* uncompress_buffer = new char[uncompress_buffer_size];
    if (uncompress((Bytef *)uncompress_buffer, &uncompress_buffer_size, (const Bytef *)(decoded_filterStr.c_str()+12), (decoded_filterStr.size() - 12)) != Z_OK) {
        delete []uncompress_buffer;
        return false;
    }
    memcpy(_arraryBits, uncompress_buffer, _bitsBufferSize);
    delete []uncompress_buffer;
    return true;
}

void BloomFilter::SetBit(uint32_t byte_offset, uint32_t bit_offset) {
    _arraryBits[byte_offset] |= bit_mask[bit_offset];
    return;
}

bool BloomFilter::TestBit(uint32_t byte_offset, uint32_t bit_offset) {
    return _arraryBits[byte_offset] & bit_mask[bit_offset];
}

void BloomFilter::GenerateHeaderInfo(char* headerInfo, uint32_t& size) const {
    if (size <= 12) {
        size = 0;
        return;
    }
    uint32_t *pointer;
    uint32_t bits_num = htonl(_bits);
    uint32_t hash_num = htonl(_hashFuncNum);
    uint32_t elements_num = htonl(_elementNum);
    uint32_t offset = 0;
    pointer = (uint32_t*)(headerInfo+offset);
    *pointer = bits_num;
    offset += sizeof(bits_num);
    pointer = (uint32_t*)(headerInfo+offset);
    *pointer = hash_num;
    offset += sizeof(hash_num);
    pointer = (uint32_t*)(headerInfo+offset);
    *pointer = elements_num;
    offset += sizeof(elements_num);
    size = offset;
}

}
