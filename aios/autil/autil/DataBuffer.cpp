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
#include "autil/DataBuffer.h"

#include <utility>

#include "autil/HashAlgorithm.h"
#include "autil/mem_pool/Pool.h"

namespace autil {

void DataBuffer::dataBufferCorrupted() const {
    AUTIL_LEGACY_THROW(DataBufferCorruptedException, "invalid data");
}

void DataBuffer::readVarint32Fallback(uint32_t &value) {
    readVarint32FallbackInline(value);
}

void DataBuffer::readVarint32Slow(uint32_t &value) {
    uint32_t b;
    uint32_t result;
    unsigned char* buffer = _pdata;

    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(buffer, _pend, sizeof(*buffer));
    b = *(buffer++); result  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(buffer, _pend, sizeof(*buffer));
    b = *(buffer++); result |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(buffer, _pend, sizeof(*buffer));
    b = *(buffer++); result |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(buffer, _pend, sizeof(*buffer));
    b = *(buffer++); result |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(buffer, _pend, sizeof(*buffer));
    b = *(buffer++); result |=  b         << 28; if (!(b & 0x80)) goto done;

done:
    value = result;
    _pdata = buffer;
}

void DataBuffer::readVarint64Fallback(uint64_t &value) {
    readVarint64FallbackInline(value);
}

void DataBuffer::readVarint64Slow(uint64_t &value) {
    uint64_t result = 0;
    int count = 0;
    uint32_t b;

    do {
        if (count == MAX_VARINT64_BYTES || _pdata ==  _pend) {
            dataBufferCorrupted();
        }
        b = *_pdata;
        result |= static_cast<uint64_t>(b & 0x7F) << (7 * count);
        ++_pdata;
        ++count;
    } while (b & 0x80);

    value = result;
}

DataBuffer* DataBuffer::findSectionBuffer(const std::string& name) {
    auto iter = _sectionMap.find(name);
    if (_sectionMap.end() == iter) {
        return NULL;
    }
    return iter->second;
}

DataBuffer* DataBuffer::declareSectionBuffer(const std::string& name) {
    DataBuffer *sectionBuf = findSectionBuffer(name);
    if (!sectionBuf) {
        sectionBuf = new DataBuffer(DEFAUTL_DATA_BUFFER_SIZE, _pool);
        _sectionMap.insert(std::make_pair(name, sectionBuf));
    }

    return sectionBuf;
}

void DataBuffer::fillData(unsigned char *pbuf) {
    size_t dataLen = getDataLen();
    memcpy(pbuf, getData(), dataLen);
}

void DataBuffer::fillSectionData(unsigned char *pbuf) {
    for (const auto &section : _sectionMap) {
        auto dataLen = section.second->getDataLen();
        memcpy(pbuf, section.second->getData(), dataLen);
        pbuf += dataLen;
    }
}

void DataBuffer::fillSectionMeta(unsigned char *pbuf, const DataBuffer &metaBuffer) {
    size_t metaLen = metaBuffer.getDataLen();
    memcpy(pbuf, metaBuffer.getData(), metaLen);
}

void DataBuffer::fillSectionHeader(unsigned char *pbuf,
                                   const DataBuffer &metaBuffer) {
    SectionHeader sectionHeader;
    size_t secMetaLen = metaBuffer.getDataLen();
    sectionHeader.checkSum =
        HashAlgorithm::hashString(
                metaBuffer.getData(), secMetaLen, 0);
    sectionHeader.metaLen = secMetaLen;
    sectionHeader.reserve = 0;

    *(SectionHeader *)(pbuf) = sectionHeader;
}

void DataBuffer::fillSectionFlag(unsigned char *pbuf) {
    *(uint32_t *)(pbuf) = SECTION_FLAG;
}

void DataBuffer::serializeToStringWithSection(std::string &resultStr) {
    // make meta buffer
    SectionMeta sectionMeta;
    DataBuffer secMetaBuffer(DEFAUTL_DATA_BUFFER_SIZE, _pool);
    size_t dataLen = getDataLen();
    size_t secDataLen = 0;
    buildSectionMeta(sectionMeta, secDataLen);
    sectionMeta.serialize(secMetaBuffer);

    // calc new mem size
    size_t memLen = dataLen + secDataLen + secMetaBuffer.getDataLen() +
                    sizeof(SectionHeader) + sizeof(SECTION_FLAG);
    unsigned char* resultStart = allocateMemory(memLen);
    if (!resultStart) {
        return;
    }

    // fill data
    unsigned char* resultEnd = resultStart + memLen;
    unsigned char* pbuf = resultStart;

    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(pbuf, resultEnd, dataLen);
    fillData(pbuf);
    pbuf += dataLen;

    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(pbuf, resultEnd, secDataLen);
    fillSectionData(pbuf);
    pbuf += secDataLen;

    size_t secMetaBufferLen = secMetaBuffer.getDataLen();
    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(pbuf, resultEnd, secMetaBufferLen);
    fillSectionMeta(pbuf, secMetaBuffer);
    pbuf += secMetaBufferLen;

    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(pbuf, resultEnd, sizeof(SectionHeader));
    fillSectionHeader(pbuf, secMetaBuffer);
    pbuf += sizeof(SectionHeader);

    AUTIL_DATABUFFER_ENSURE_VALID_ACCESS(pbuf, resultEnd, sizeof(SECTION_FLAG));
    fillSectionFlag(pbuf);

    resultStr.assign((const char*)resultStart, memLen);
    if (!_pool) {
        delete[] resultStart;
    }
}

unsigned char* DataBuffer::allocateMemory(const size_t memLen) {
    if (_pool) {
        return (unsigned char*)_pool->allocate(memLen);
    } else {
        return new unsigned char[memLen];
    }
}

bool DataBuffer::checkSectionFlag(const unsigned char* pbuf) {
    uint32_t flag = *(uint32_t*) pbuf;
    if (SECTION_FLAG != flag) {
        return false;
    }
    return true;
}

bool DataBuffer::extractAndCheckSectionHeader(SectionHeader &header,
        const unsigned char* pbuf)
{
    header = *(SectionHeader *) pbuf;
    if (header.metaLen > (pbuf - _pdata)) {
        return false;
    }

    uint32_t checkSum =
        HashAlgorithm::hashString(
                (const char*)(pbuf - header.metaLen), header.metaLen, 0);
    if (checkSum != header.checkSum) {
        return false;
    }
    return true;
}

bool DataBuffer::extractAndCheckSectionMeta(SectionMeta &secMeta, size_t metaLen,
                                        const unsigned char *metaData)
{
    DataBuffer secMetaBuffer((void*)metaData, metaLen, _pool);
    secMetaBuffer.read(secMeta);
    if (!secMeta.validate()) {
        return false;
    }
    return true;
}

void DataBuffer::extractSectionBuffer(const SectionMeta &secMeta,
                                  size_t &sectionDataLen)
{
    sectionDataLen = 0;
    const auto &secInfoMap = secMeta.getSectionInfoVec();
    for (const auto &sec : secInfoMap) {
        DataBuffer *secDataBuffer = new DataBuffer(_pdata + sec.start,
                sec.length, _pool);
        _sectionMap.insert(std::make_pair(sec.name, secDataBuffer));
        sectionDataLen += sec.length;
    }
}

void DataBuffer::deserializeFromStringWithSection(const std::string &str,
        mem_pool::Pool *pool,
        bool needCopy)
{
    // init mem
    initByString(str, pool, needCopy);

    // get header
    unsigned char* pbuf = _pfree;
    AUTIL_DATABUFFER_VALID_AND_MOVE_BACKWARD(_pdata, pbuf, sizeof(SECTION_FLAG))
    if (!checkSectionFlag(pbuf)) {
        return;
    }
    AUTIL_DATABUFFER_VALID_AND_MOVE_BACKWARD(_pdata, pbuf, sizeof(SectionHeader))
    SectionHeader header;
    if (!extractAndCheckSectionHeader(header, pbuf)) {
        return;
    }

    // get meta and deserialize
    AUTIL_DATABUFFER_VALID_AND_MOVE_BACKWARD(_pdata, pbuf, header.metaLen);
    SectionMeta secMeta;
    if (!extractAndCheckSectionMeta(secMeta, header.metaLen, pbuf)) {
        return;
    }

    // build extend databuffer
    size_t secDataLen = 0u;
    extractSectionBuffer(secMeta, secDataLen);
    AUTIL_DATABUFFER_VALID_AND_MOVE_BACKWARD(_pdata, pbuf, secDataLen);
    _pend = _pfree = pbuf;
}

void DataBuffer::init(int len, autil::mem_pool::Pool *pool) {
    _pool = pool;
    _pfree = _pdata = _pstart = allocateMemory(len);
    _pend = _pstart + len;
    _needReleaseMemory = true;
}

void DataBuffer::init(void* data, size_t dataLen, autil::mem_pool::Pool *pool) {
    _pool = pool;
    _pdata = _pstart = (unsigned char*)data;
    _pend = _pfree = _pdata + dataLen;
    _needReleaseMemory = false;
}

void DataBuffer::reset() {
    for (auto section : _sectionMap) {
        delete section.second;
        section.second = NULL;
    }
    _sectionMap.clear();
    if (!_needReleaseMemory) {
        _pstart = _pfree = _pend = NULL;
        _pool = NULL;
        return;
    }
    if (_pstart) {
        if (NULL == _pool) {
            delete[] _pstart;
        }
    }
    _pstart = _pfree = _pend = NULL;
    _pool = NULL;
    _needReleaseMemory = false;
}

void DataBuffer::buildSectionMeta(SectionMeta &secMeta, size_t &secDataLen) {
    size_t start = getDataLen();
    secDataLen = 0;
    for (const auto &section : _sectionMap) {
        auto dataLen = section.second->getDataLen();
        secMeta.addSectionInfo(section.first, start,
                               dataLen);
        start += dataLen;
        secDataLen += dataLen;
    }
}

void DataBuffer::initByString(const std::string &str, mem_pool::Pool *pool,
                              bool needCopy)
{
    reset();
    size_t len = str.size();
    if (needCopy) {
        init(len, pool);
        memcpy(_pdata, str.c_str(), len);
        _pend = _pfree = _pdata + len;
    } else {
        init((void *)str.c_str(), str.size(), pool);
    }
}

void DataBuffer::_writeConstString(const StringView &value) {
    uint32_t len = value.size();
    write(len);
    writeBytes(value.data(), value.size());
}

void DataBuffer::_readConstString(StringView &value) {
    uint32_t len = 0;
    read(len);
    const char *data = reinterpret_cast<const char*>(readNoCopy(len));
    value = StringView(data, len);
}

void DataBuffer::makeConstString(StringView &value, char *data, uint32_t len) {
    value = StringView(data, len);
}

void SectionMeta::SectionInfo::serialize(DataBuffer &dataBuffer) const {
    dataBuffer.write(name);
    dataBuffer.write(start);
    dataBuffer.write(length);
}

void SectionMeta::SectionInfo::deserialize(DataBuffer &dataBuffer) {
    dataBuffer.read(name);
    dataBuffer.read(start);
    dataBuffer.read(length);
}

void SectionMeta::serialize(DataBuffer &dataBuffer) const {
    dataBuffer.write(_sectionInfoVec);
}

void SectionMeta::deserialize(DataBuffer &dataBuffer) {
    dataBuffer.read(_sectionInfoVec);
}

}
