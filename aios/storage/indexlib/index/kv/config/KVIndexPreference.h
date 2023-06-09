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
#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/index/kv/Constant.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::config {

class KVIndexPreference : public autil::legacy::Jsonizable
{
public:
    // TODO: support hash type
    class HashDictParam : public autil::legacy::Jsonizable
    {
    public:
        HashDictParam() {}
        HashDictParam(const std::string& hashType) : _hashType(hashType) {}
        HashDictParam(const std::string& hashType, int32_t occupancyPct)
            : _hashType(hashType)
            , _occupancyPct(occupancyPct)
        {
        }
        HashDictParam(const std::string& hashType, int32_t occupancyPct, bool mergeUsePreciseCount,
                      bool enableCompactHashKey, bool enableShortenOffset)
            : _hashType(hashType)
            , _occupancyPct(occupancyPct)
            , _mergeUsePreciseCount(mergeUsePreciseCount)
            , _enableCompactHashKey(enableCompactHashKey)
            , _enableShortenOffset(enableShortenOffset)
        {
        }

        HashDictParam(const HashDictParam& other)
            : _hashType(other._hashType)
            , _occupancyPct(other._occupancyPct)
            , _maxValueSizeForShortOffset(other._maxValueSizeForShortOffset)
            , _mergeUsePreciseCount(other._mergeUsePreciseCount)
            , _enableCompactHashKey(other._enableCompactHashKey)
            , _enableShortenOffset(other._enableShortenOffset)
        {
        }

        const std::string& GetHashType() const { return _hashType; }
        void SetHashType(const std::string& hashType) { _hashType = hashType; }
        int32_t GetOccupancyPct() const { return _occupancyPct; }
        void SetOccupancyPct(int32_t occupancyPct) { _occupancyPct = occupancyPct; }

        virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("type", _hashType, _hashType);
            json.Jsonize("occupancy_pct", _occupancyPct, _occupancyPct);
            json.Jsonize("merge_use_precise_count", _mergeUsePreciseCount, _mergeUsePreciseCount);
            json.Jsonize("enable_compact_hash_key", _enableCompactHashKey, _enableCompactHashKey);
            json.Jsonize("enable_shorten_offset", _enableShortenOffset, _enableShortenOffset);
            // for test
            json.Jsonize("max_value_size_for_short_offset", _maxValueSizeForShortOffset, _maxValueSizeForShortOffset);
        }
        void Check() const
        {
            if (_hashType.empty()) {
                return;
            }
            if (_hashType != "dense" && _hashType != "separate_chain" && _hashType != "cuckoo") {
                INDEXLIB_FATAL_ERROR(BadParameter, "unsupported hash dict type [%s]", _hashType.c_str());
            }
            if (_occupancyPct <= 0 || _occupancyPct > 100) {
                INDEXLIB_FATAL_ERROR(BadParameter, "invalid occupancy_pct[%d]", _occupancyPct);
            }
        }

        bool UsePreciseCountWhenMerge() const { return _mergeUsePreciseCount; }
        bool HasEnableCompactHashKey() const { return _enableCompactHashKey; }
        bool HasEnableShortenOffset() const { return _enableShortenOffset; }

        void SetEnableCompactHashKey(bool flag) { _enableCompactHashKey = flag; }
        void SetEnableShortenOffset(bool flag) { _enableShortenOffset = flag; }

        void SetMaxValueSizeForShortOffset(size_t size) { _maxValueSizeForShortOffset = size; }
        size_t GetMaxValueSizeForShortOffset() const { return _maxValueSizeForShortOffset; }

    private:
        std::string _hashType;
        int32_t _occupancyPct = 50;
        size_t _maxValueSizeForShortOffset = index::DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET;
        bool _mergeUsePreciseCount = true;
        bool _enableCompactHashKey = true;
        bool _enableShortenOffset = true;
    };

    class ValueParam : public autil::legacy::Jsonizable
    {
    public:
        ValueParam() {}
        ValueParam(bool encode, const std::string& fileCompType) : _encode(encode), _fileCompType(fileCompType) {}
        ValueParam(bool encode, const std::string& fileCompType, size_t fileCompBuffSize)
            : _encode(encode)
            , _fileCompType(fileCompType)
            , _fileCompBuffSize(fileCompBuffSize)
        {
        }
        ValueParam(bool encode, const std::string& fileCompType, size_t fileCompBuffSize, bool fixLenAutoInline)
            : _encode(encode)
            , _fixLenAutoInline(fixLenAutoInline)
            , _fileCompType(fileCompType)
            , _fileCompBuffSize(fileCompBuffSize)
        {
        }

        bool IsEncode() const { return _encode; }

        bool IsValueImpact() const { return _valueImpact; }
        void EnableValueImpact(bool flag)
        {
            _valueImpact = flag;
            _plainFormat = _plainFormat && !_valueImpact;
        }

        bool IsPlainFormat() const { return _plainFormat; }
        void EnablePlainFormat(bool flag)
        {
            _plainFormat = flag;
            _valueImpact = _valueImpact && !_plainFormat;
        }

        bool EnableFileCompress() const { return !_fileCompType.empty(); }
        const std::string& GetFileCompressType() const { return _fileCompType; }
        void SetFileCompressType(const std::string& compressorType) { _fileCompType = compressorType; }
        size_t GetFileCompressBufferSize() const { return _fileCompBuffSize; }
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        void Check() const;
        bool IsFixLenAutoInline() const { return _fixLenAutoInline; }
        void EnableFixLenAutoInline() { _fixLenAutoInline = true; }
        void SetEnableFixLenAutoInline(bool flag) { _fixLenAutoInline = flag; }
        const util::KeyValueMap& GetFileCompressParameter() const { return _fileCompParams; }
        void SetCompressParameter(const std::string& key, const std::string& value) { _fileCompParams[key] = value; }

    private:
        static const size_t DEFAULT_FILE_COMP_BUFF_SIZE = 4 * 1024;

    private:
        bool _encode = false;
        bool _valueImpact = false;
        bool _plainFormat = false;
        bool _fixLenAutoInline = false;
        std::string _fileCompType;
        size_t _fileCompBuffSize = DEFAULT_FILE_COMP_BUFF_SIZE;
        util::KeyValueMap _fileCompParams;
    };

public:
    KVIndexPreference();
    virtual ~KVIndexPreference();

public:
    IndexPreferenceType GetType() const { return _type; }
    const HashDictParam& GetHashDictParam() const { return _hashParam; }
    HashDictParam& GetHashDictParam() { return _hashParam; }
    const ValueParam& GetValueParam() const { return _valueParam; }
    ValueParam& GetValueParam() { return _valueParam; }

    // for test
    void SetType(IndexPreferenceType type) { _type = type; }
    void SetHashDictParam(const HashDictParam& param) { _hashParam = param; }
    void SetValueParam(const ValueParam& param) { _valueParam = param; }

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual void Check() const;

protected:
    IndexPreferenceType StrToPreferenceType(const std::string& typeStr) const;
    std::string PreferenceTypeToStr(IndexPreferenceType type) const;

private:
    void InitDefaultParams(IndexPreferenceType type);

public:
    inline static const std::string PREFERENCE_TYPE_NAME = "type";
    inline static const std::string PREFERENCE_PARAMS_NAME = "parameters";
    inline static const std::string PERF_PREFERENCE_TYPE = "PERF";
    inline static const std::string STORE_PREFERENCE_TYPE = "STORE";

protected:
    IndexPreferenceType _type;
    HashDictParam _hashParam;
    ValueParam _valueParam;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
