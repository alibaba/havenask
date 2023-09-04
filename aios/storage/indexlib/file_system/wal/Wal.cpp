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
#include "indexlib/file_system/wal/Wal.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <string.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/CRC32C.h"
#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"
#include "indexlib/util/buffer_compressor/Lz4Compressor.h"
#include "indexlib/util/buffer_compressor/Lz4HcCompressor.h"
#include "indexlib/util/buffer_compressor/SnappyCompressor.h"
#include "indexlib/util/buffer_compressor/ZlibCompressor.h"
#include "indexlib/util/buffer_compressor/ZlibDefaultCompressor.h"
#include "indexlib/util/buffer_compressor/ZstdCompressor.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, WAL);

std::map<WAL::CompressType, indexlib::util::BufferCompressorPtr> WAL::WALOperator::_compressors;

AUTIL_LOG_SETUP(indexlib.file_system, WAL::WALOperator);
AUTIL_LOG_SETUP(indexlib.file_system, WAL::Writer);
AUTIL_LOG_SETUP(indexlib.file_system, WAL::Reader);

bool WAL::IsValidWALDirName(const std::string& fileName)
{
    return autil::StringUtil::startsWith(fileName, WAL::wal_prefix) &&
           autil::StringUtil::endsWith(fileName, WAL::wal_suffix);
}

bool WAL::WALOperator::Init()
{
    std::vector<std::pair<WAL::CompressType, std::string>> arr;
    arr.emplace_back(std::make_pair(WAL::CompressType::CompressionKind_ZLIB, util::ZlibCompressor::COMPRESSOR_NAME));
    arr.emplace_back(
        std::make_pair(WAL::CompressType::CompressionKind_ZLIB_DEFAULT, util::ZlibDefaultCompressor::COMPRESSOR_NAME));
    arr.emplace_back(
        std::make_pair(WAL::CompressType::CompressionKind_SNAPPY, util::SnappyCompressor::COMPRESSOR_NAME));
    arr.emplace_back(std::make_pair(WAL::CompressType::CompressionKind_LZ4, util::Lz4Compressor::COMPRESSOR_NAME));
    arr.emplace_back(std::make_pair(WAL::CompressType::CompressionKind_LZ4_HC, util::Lz4HcCompressor::COMPRESSOR_NAME));
    arr.emplace_back(std::make_pair(WAL::CompressType::CompressionKind_ZSTD, util::ZstdCompressor::COMPRESSOR_NAME));

    for (const auto& v : arr) {
        util::BufferCompressorPtr ptr(util::BufferCompressorCreator::CreateBufferCompressor(v.second));
        if (ptr == nullptr) {
            AUTIL_LOG(ERROR, "unsupport comrpess type, name[%s]", v.second.c_str());
            return false;
        }
        _compressors[v.first] = ptr;
    }
    return true;
}

// namespace wal defined in proto
// namespace WAL defined in class WAL
bool WAL::WALOperator::ToPBType(const WAL::CompressType& src, ::indexlib::proto::CompressType& dst)
{
    switch (src) {
    case WAL::CompressType::CompressionKind_NONE:
        dst = ::indexlib::proto::CompressType::CompressionKind_NONE;
        break;
    case WAL::CompressType::CompressionKind_ZLIB:
        dst = ::indexlib::proto::CompressType::CompressionKind_ZLIB;
        break;
    case WAL::CompressType::CompressionKind_ZLIB_DEFAULT:
        dst = ::indexlib::proto::CompressType::CompressionKind_ZLIB_DEFAULT;
        break;
    case WAL::CompressType::CompressionKind_SNAPPY:
        dst = ::indexlib::proto::CompressType::CompressionKind_SNAPPY;
        break;
    case WAL::CompressType::CompressionKind_LZ4:
        dst = ::indexlib::proto::CompressType::CompressionKind_LZ4;
        break;
    case WAL::CompressType::CompressionKind_LZ4_HC:
        dst = ::indexlib::proto::CompressType::CompressionKind_LZ4_HC;
        break;
    case WAL::CompressType::CompressionKind_ZSTD:
        dst = ::indexlib::proto::CompressType::CompressionKind_ZSTD;
        break;
    default:
        AUTIL_LOG(ERROR, "Unsupport wal compress type[%d]", static_cast<int>(src));
        return false;
    }
    return true;
}

bool WAL::WALOperator::ToWALType(const ::indexlib::proto::CompressType& src, WAL::CompressType& dst)
{
    switch (src) {
    case ::indexlib::proto::CompressionKind_NONE:
        dst = WAL::CompressType::CompressionKind_NONE;
        break;
    case ::indexlib::proto::CompressionKind_ZLIB:
        dst = WAL::CompressType::CompressionKind_ZLIB;
        break;
    case ::indexlib::proto::CompressionKind_ZLIB_DEFAULT:
        dst = WAL::CompressType::CompressionKind_ZLIB_DEFAULT;
        break;
    case ::indexlib::proto::CompressionKind_SNAPPY:
        dst = WAL::CompressType::CompressionKind_SNAPPY;
        break;
    case ::indexlib::proto::CompressionKind_LZ4:
        dst = WAL::CompressType::CompressionKind_LZ4;
        break;
    case ::indexlib::proto::CompressionKind_LZ4_HC:
        dst = WAL::CompressType::CompressionKind_LZ4_HC;
        break;
    case ::indexlib::proto::CompressionKind_ZSTD:
        dst = WAL::CompressType::CompressionKind_ZSTD;
        break;
    default:
        AUTIL_LOG(ERROR, "Unsupport wal compress type[%d]", static_cast<int>(src));
        return false;
    }
    return true;
}

const util::BufferCompressorPtr WAL::WALOperator::GetCompressor(const WAL::CompressType& type)
{
    auto it = _compressors.find(type);
    if (it == _compressors.end()) {
        return util::BufferCompressorPtr();
    }
    return it->second;
}

WAL::Writer::~Writer()
{
    // TODO: add a Close method to process error
    auto fslibEc = _file->sync();
    if (fslibEc != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "sync [%s] failed", _file->getFileName());
    }
    fslibEc = _file->close();
    if (fslibEc != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "close [%s] failed", _file->getFileName());
    }
}

bool WAL::Writer::Compress(const std::string& record, std::string& compressData)
{
    if (_compressType != CompressType::CompressionKind_NONE) {
        auto compressor = GetCompressor(_compressType);
        if (compressor == nullptr) {
            AUTIL_LOG(ERROR, "get compressor failed, compress type[%d]", static_cast<int>(_compressType));
            return false;
        }
        compressor->Reset();
        compressor->AddDataToBufferIn(record.data(), record.size());
        if (!compressor->Compress()) {
            AUTIL_LOG(ERROR, "compress data failed, compressName[%s], data[%s]",
                      compressor->GetCompressorName().c_str(), record.c_str());
            return false;
        }
        auto data = compressor->GetBufferOut();
        auto len = compressor->GetBufferOutLen();
        compressData.replace(0, compressData.size(), data, len);
    } else {
        compressData = std::move(record);
    }
    return true;
}

uint32_t WAL::Writer::GetDataCRC(const std::string& record)
{
    uint32_t crc = autil::CRC32C::Extend(0, record.data(), record.size());
    return autil::CRC32C::Mask(crc); // Adjust for storage
}

bool WAL::Writer::AppendRecord(const std::string& record)
{
    ::indexlib::proto::WalRecordMeta meta;
    meta.set_offset(_offset);
    ::indexlib::proto::CompressType type;
    if (!ToPBType(_compressType, type)) {
        AUTIL_LOG(ERROR, "convert to pb compress type failed");
        return false;
    }
    meta.set_compresstype(type);

    std::string compressData;
    if (!Compress(record, compressData)) {
        AUTIL_LOG(ERROR, "compress data failed");
        return false;
    }
    meta.set_datalen(compressData.size());
    meta.set_crc(GetDataCRC(compressData));

    std::string pbStr;
    if (!meta.SerializeToString(&pbStr)) {
        AUTIL_LOG(ERROR, "serialize wal meta to string failed, meta[%s]", meta.ShortDebugString().c_str());
        return false;
    }

    size_t size = sizeof(uint32_t) + pbStr.size() + compressData.size();
    if (size > _buffer->size()) {
        AUTIL_LOG(INFO, "WAL writer buffer expand size from [%zu] bytes to [%zu] bytes", _buffer->size(), size);
        _buffer->resize(size);
    }

    uint32_t off = 0;
    *(uint32_t*)(_buffer->data()) = pbStr.size();
    off += sizeof(uint32_t);
    memcpy(_buffer->data() + off, pbStr.data(), pbStr.size());
    off += pbStr.size();
    memcpy(_buffer->data() + off, compressData.data(), compressData.size());
    off += compressData.size();

    auto writeBytes = _file->write(_buffer->data(), off);
    if (writeBytes < 0 || writeBytes != off) {
        AUTIL_LOG(ERROR, "write data to file failed. file[%s], expected bytes[%u], write bytes[%ld]",
                  _file->getFileName(), off, writeBytes);
        return false;
    }

    auto ec = _file->flush();
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "flush failed, ec[%d] file[%s]", static_cast<int>(ec), _file->getFileName());
        return false;
    }

    _offset += writeBytes;
    AUTIL_LOG(DEBUG, "meta[%s] current offset[%lu]", meta.ShortDebugString().c_str(), _offset);
    // std::cout << "meta:" << meta.ShortDebugString() << " offset:" << _offset << std::endl;
    return true;
}

bool WAL::Reader::DeCompress(const WAL::CompressType& type, const char* rawData, uint32_t len,
                             std::string& compressData) const
{
    if (type != CompressType::CompressionKind_NONE) {
        auto compressor = GetCompressor(type);
        if (compressor == nullptr) {
            AUTIL_LOG(ERROR, "get de-compressor failed, compress type[%d]", static_cast<int>(type));
            return false;
        }
        compressor->Reset();
        compressor->SetBufferInLen(len);
        compressor->AddDataToBufferIn(rawData, len);
        if (!compressor->Decompress()) {
            AUTIL_LOG(ERROR, "de-compress data failed, compress type[%d] name[%s]", static_cast<int>(type),
                      compressor->GetCompressorName().c_str());
            return false;
        }
        auto data = compressor->GetBufferOut();
        auto len = compressor->GetBufferOutLen();
        compressData.replace(0, compressData.size(), data, len);
    } else {
        compressData.replace(0, compressData.size(), rawData, len);
    }
    return true;
}

bool WAL::Reader::RotateNextBlock(size_t& leftLen, int64_t requiredBytes)
{
    auto len = _endOffsetInBlock - _offsetInBlock;
    if (len + requiredBytes > _buffer->size()) {
        AUTIL_LOG(INFO, "WAL reader buffer expand size from [%zu] bytes to [%zu] bytes", _buffer->size(),
                  len + requiredBytes);
        _buffer->resize(len + requiredBytes);
    }
    if (len != 0) {
        std::copy(_buffer->begin() + _offsetInBlock, _buffer->end(), _buffer->begin());
    }
    _skipOffset += _offsetInBlock;
    _offsetInBlock = len;
    if (_skipOffset + len + requiredBytes > _fileLength) {
        AUTIL_LOG(WARN, "read record at [%lu], exceed file length [%lu], skip this record, set file as eof",
                  _skipOffset + len + requiredBytes, _fileLength);
        _eof = true;
        return false;
    }
    len = _file->pread(/*buffer*/ _buffer->data() + _offsetInBlock,
                       /*length*/ _buffer->size() - _offsetInBlock, /*offset*/ _skipOffset + len);
    if (len < 0) {
        AUTIL_LOG(ERROR, "read failed, file[%s]", _file->getFileName());
        return false;
    } else if (len == 0) {
        AUTIL_LOG(INFO, "come to file end, file[%s]", _file->getFileName());
        _eof = true;
        return false;
    } else if (len < requiredBytes) {
        AUTIL_LOG(ERROR, "read failed, file[%s], read [%ld], required [%ld]", _file->getFileName(), len, requiredBytes);
        return false;
    } else {
        // read success
    }
    _endOffsetInBlock = len + _offsetInBlock;
    leftLen = len + _offsetInBlock;
    _offsetInBlock = 0;
    return true;
}

bool WAL::Reader::ReadRecord(std::string& record)
{
    if (_eof) {
        return false;
    }

    // format like: |pb_meta_len|pb_meta|crc(data)|
    auto leftLen = _endOffsetInBlock - _offsetInBlock;
    if (leftLen < _headerLen) {
        // rotate next block
        if (!RotateNextBlock(leftLen, _headerLen - leftLen)) {
            AUTIL_LOG(DEBUG, "read next block failed, file[%s]", _file->getFileName());
            return false;
        }
    }

    // parse pb_len
    uint32_t pb_len = *(uint32_t*)(_buffer->data() + _offsetInBlock);
    _offsetInBlock += _headerLen;
    leftLen -= _headerLen;

    if (leftLen < pb_len) {
        // rotate next block
        if (!RotateNextBlock(leftLen, pb_len - leftLen)) {
            AUTIL_LOG(ERROR, "read next block failed, file[%s]", _file->getFileName());
            return false;
        }
    }

    std::string pbStr(_buffer->data() + _offsetInBlock, pb_len);
    _offsetInBlock += pb_len;
    leftLen -= pb_len;
    // get pb
    ::indexlib::proto::WalRecordMeta meta;
    if (!meta.ParseFromString(pbStr)) {
        AUTIL_LOG(ERROR, "parse pb from string failed, str[%s]", pbStr.c_str());
        return false;
    }

    if (leftLen < meta.datalen()) {
        // rotate next block
        if (!RotateNextBlock(leftLen, meta.datalen() - leftLen)) {
            AUTIL_LOG(ERROR, "read next block failed, file[%s]", _file->getFileName());
            return false;
        }
    }

    // check crc
    if (_isCheckSum) {
        uint32_t expected_crc = autil::CRC32C::Unmask(meta.crc());
        uint32_t actual_crc = autil::CRC32C::Value(_buffer->data() + _offsetInBlock, meta.datalen());
        if (actual_crc != expected_crc) {
            AUTIL_LOG(ERROR, "mismatch crc, expect[%u] actual[%u] origin[%u] file[%s]", expected_crc, actual_crc,
                      meta.crc(), _file->getFileName());
            return false;
        }
    }
    WAL::CompressType compressType;
    if (!ToWALType(meta.compresstype(), compressType)) {
        AUTIL_LOG(ERROR, "unsupport compress type[%d] file[%s]", static_cast<int>(meta.compresstype()),
                  _file->getFileName());
        return false;
    }
    // de-compress data
    if (!DeCompress(compressType, _buffer->data() + _offsetInBlock, meta.datalen(), record)) {
        AUTIL_LOG(ERROR, "de-compress data failed, file[%s]", _file->getFileName());
        return false;
    }
    _offsetInBlock += meta.datalen();
    leftLen -= meta.datalen();
    _offsetInFile += _headerLen + pb_len + meta.datalen();
    return true;
}

bool WAL::Init()
{
    bool isExist = false;
    if (FslibWrapper::IsExist(_walOpt.workDir, isExist) != FSEC_OK) {
        AUTIL_LOG(ERROR, "IsExist path failed, file[%s]", _walOpt.workDir.c_str());
        return false;
    }
    if (!isExist) {
        auto ec = FslibWrapper::MkDir(_walOpt.workDir, true, true).Code();
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "mkdir dir failed, dir[%s]", _walOpt.workDir.c_str());
            return false;
        }
    }
    if (!LoadWALFiles()) {
        AUTIL_LOG(ERROR, "init wal failed");
        return false;
    }

    if (!SeekRecoverPosition(_walOpt.recoverPos)) {
        AUTIL_LOG(ERROR, "seek recover position failed, pos[%ld]", _walOpt.recoverPos);
        return false;
    }

    uint64_t start_offset = 0;
    if (_walFiles.size() > 0) {
        auto it = _walFiles.rbegin();
        start_offset = it->first + it->second.physicalSize;
    }

    if (!OpenWriter(start_offset)) {
        AUTIL_LOG(ERROR, "Init failed");
        return false;
    }
    return true;
}

bool WAL::Recycle()
{
    auto ec = FslibWrapper::DeleteDir(_walOpt.workDir, DeleteOption::NoFence(false)).Code();
    if (ec == FSEC_NOENT) {
        AUTIL_LOG(WARN, "delete dir not exist, dir[%s]", _walOpt.workDir.c_str());
        return true;
    }
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "delete dir failed, dir[%s]", _walOpt.workDir.c_str());
        return false;
    }
    _walFiles.clear();
    _writer.reset();
    _reader.reset();
    return true;
}

std::pair<std::shared_ptr<fslib::fs::File>, size_t> WAL::OpenFile(size_t offset, bool useDirectIO, fslib::Flag mode)
{
    auto dir = _walOpt.workDir;
    if (dir[dir.size() - 1] != '/') {
        dir += '/';
    }

    auto fileName = dir + wal_prefix + std::to_string(offset) + wal_suffix;
    std::shared_ptr<fslib::fs::File> file(fslib::fs::FileSystem::openFile(fileName, mode, useDirectIO));
    if (!file) {
        AUTIL_LOG(ERROR, "open file[%s] failed", fileName.c_str());
        return {std::shared_ptr<fslib::fs::File>(), 0};
    }
    fslib::FileMeta meta;
    auto ec = fslib::fs::FileSystem::getFileMeta(fileName, meta);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta for [%s] failed, error code: %d", fileName.c_str(), ec);
        return {std::shared_ptr<fslib::fs::File>(), 0};
    }
    if (meta.fileLength < 0) {
        AUTIL_LOG(ERROR, "file length of [%s][%ld] invalid", fileName.c_str(), meta.fileLength);
        return {std::shared_ptr<fslib::fs::File>(), 0};
    }
    return {file, static_cast<size_t>(meta.fileLength)};
}

bool WAL::OpenWriter(size_t offset)
{
    auto [file, fileLength] = OpenFile(offset, _walOpt.isWriteDirectIO, fslib::WRITE);
    if (!file) {
        AUTIL_LOG(ERROR, "open writer failed");
        return false;
    }
    _writer.reset(new Writer(file, offset, _walOpt.compressType));
    return true;
}

bool WAL::OpenReader(size_t begin_pos, size_t offset)
{
    auto [file, fileLength] = OpenFile(begin_pos, _walOpt.isReadDirectIO, fslib::READ);
    if (!file) {
        AUTIL_LOG(ERROR, "open reader failed");
        return false;
    }
    _reader.reset(new Reader(file, fileLength, _walOpt.isCheckSum, offset, begin_pos));
    return true;
}

bool WAL::AppendRecord(const std::string& record)
{
    if (_writer == nullptr) {
        return false; // make sure error_code
    }

    if (!_writer->AppendRecord(record)) {
        AUTIL_LOG(ERROR, "append record failed, file[%s]", _writer->GetWriterFileName());
        if (++_continuousAppendFails > CONTINUOUS_APPEND_FAIL_LIMITS) {
            AUTIL_LOG(ERROR, "continuous append fail [%lu] exceed limits [%lu]", _continuousAppendFails,
                      CONTINUOUS_APPEND_FAIL_LIMITS);
            return false;
        }
        OpenWriter(_writer->LastNextFileOffset());
        return false;
    }

    if (_writer->LastOffset() >= MAX_WAL_FILE_SIZE) {
        // open next wal file
        OpenWriter(_writer->LastNextFileOffset());
    }
    _continuousAppendFails = 0;
    return true;
}

bool WAL::ReadRecord(std::string& record)
{
    if (_isRecovered) {
        AUTIL_LOG(INFO, "already recovered");
        return true;
    }
    if (_walFiles.empty()) {
        AUTIL_LOG(INFO, "empty wal files, no data to read");
        return true;
    }
    if (_reader == nullptr) {
        AUTIL_LOG(ERROR, "empty wal reader, maybe not init");
        return false;
    }
    if (!_reader->ReadRecord(record)) {
        if (_reader->IsEof()) {
            // if _reader is last wal file?
            auto currentIter = _walFiles.find(_reader->BeginOffset());
            assert(currentIter != _walFiles.end());
            auto nextIter = ++currentIter;
            if (nextIter == _walFiles.end()) {
                // last file
                AUTIL_LOG(INFO, "all data recovered");
                _isRecovered = true;
                return true;
            }

            if (!OpenReader(nextIter->first, 0)) {
                AUTIL_LOG(ERROR, "read record failed: open reader");
                return false;
            }
            if (!_reader->ReadRecord(record)) {
                if (_reader->IsEof()) {
                    AUTIL_LOG(INFO, "No need recover, last file is empty");
                    return true;
                } else {
                    AUTIL_LOG(WARN, "ReadRecord failed.");
                    return false;
                }
            } else {
                return true;
            }
        }
        return false;
    }
    return true;
}

bool WAL::ReOpen(bool removeOldFile)
{
    std::string fileName = _writer->GetWriterFileName(); // use std::string instead of const char* for safe
    if (!OpenWriter(_writer->LastNextFileOffset())) {
        AUTIL_LOG(ERROR, "reopen failed");
        return false;
    }

    if (removeOldFile) {
        auto ec = fslib::fs::FileSystem::removeFile(fileName);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "remove file failed, ec[%d] file:[%s]", ec, fileName.c_str());
            return false;
        }
    }

    AUTIL_LOG(INFO, "Reopen file success, file[%s]", fileName.c_str());
    return true;
}

namespace {
bool begins_with(const std::string& str, const std::string& prefix)
{
    if (str.compare(0, prefix.size(), prefix) == 0) {
        return true;
    }
    return false;
}
} // namespace

bool WAL::LoadWALFiles()
{
    fslib::RichFileList files;
    auto ec = fslib::fs::FileSystem::listDir(_walOpt.workDir, files);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "list dir failed, ec[%d] dir:[%s]", ec, _walOpt.workDir.c_str());
        return false;
    }
    for (const auto& f : files) {
        if (!f.isDir && begins_with(f.path, wal_prefix)) {
            if (f.path.size() <= std::size(wal_prefix) - 1) {
                AUTIL_LOG(WARN, "Invalid file[%s]", f.path.c_str());
                continue;
            }
            auto offset = std::stoull(f.path.substr(std::size(wal_prefix) - 1));
            _walFiles.insert(std::make_pair(offset, f));
        }
    }
    return true;
}

bool WAL::SeekRecoverPosition(int64_t recoverPos)
{
    if (_walFiles.size() == 0) {
        // no need recover
        _isRecovered = true;
        return true;
    }

    if (recoverPos <= DEFAULT_RECOVER_POSITION) {
        return OpenReader(_walFiles.begin()->first, 0);
    }

    uint64_t currentMinPos = _walFiles.begin()->first;
    if (recoverPos < static_cast<int64_t>(currentMinPos)) {
        AUTIL_LOG(WARN, "recover pos already exist, maybe lost some data. recover[%ld] currentMin[%lu]", recoverPos,
                  currentMinPos);
        return OpenReader(_walFiles.begin()->first, 0);
    }

    uint64_t last_begin_pos = currentMinPos;
    auto it = _walFiles.begin();
    for (; it != _walFiles.end(); ++it) {
        if (static_cast<int64_t>(it->first) > recoverPos) {
            break;
        }
        last_begin_pos = it->first;
    }

    if (it == _walFiles.end()) {
        auto currentMaxPos = last_begin_pos + _walFiles.rbegin()->first;
        if (static_cast<uint64_t>(recoverPos) > currentMaxPos) {
            AUTIL_LOG(WARN, "recover pos not exist, maybe lost some data. recover[%ld] currentMin[%lu]", recoverPos,
                      currentMinPos);
            return false;
        }
    }
    return OpenReader(last_begin_pos, recoverPos - last_begin_pos);
}
}} // namespace indexlib::file_system
