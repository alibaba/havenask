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
#include "indexlib/index/orc/OrcWriterOptions.h"

#include "indexlib/util/Exception.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, OrcWriterOptions);

OrcWriterOptions::OrcWriterOptions() {}

OrcWriterOptions::~OrcWriterOptions() {}

orc::CompressionKind OrcWriterOptions::StrToCompressionKind(const std::string& compression)
{
    if (compression == "none") {
        return orc::CompressionKind::CompressionKind_NONE;
    } else if (compression == "zlib") {
        return orc::CompressionKind::CompressionKind_ZLIB;
    } else if (compression == "snappy") {
        return orc::CompressionKind::CompressionKind_SNAPPY;
    } else if (compression == "lzo") {
        return orc::CompressionKind::CompressionKind_LZO;
    } else if (compression == "lz4") {
        return orc::CompressionKind::CompressionKind_LZ4;
    } else if (compression == "zstd") {
        return orc::CompressionKind::CompressionKind_ZSTD;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid compression kind [%s].", compression.c_str());
    }
}

std::string OrcWriterOptions::CompressionKindToStr(orc::CompressionKind compression)
{
    if (compression == orc::CompressionKind::CompressionKind_NONE) {
        return "none";
    } else if (compression == orc::CompressionKind::CompressionKind_ZLIB) {
        return "zlib";
    } else if (compression == orc::CompressionKind::CompressionKind_SNAPPY) {
        return "snappy";
    } else if (compression == orc::CompressionKind::CompressionKind_LZO) {
        return "lzo";
    } else if (compression == orc::CompressionKind::CompressionKind_LZ4) {
        return "lz4";
    } else if (compression == orc::CompressionKind::CompressionKind_ZSTD) {
        return "zstd";
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid compression kind to str.");
    }
}

orc::EncodingStrategy OrcWriterOptions::StrToEncodingStrategy(const std::string& strategy)
{
    if (strategy == "speed") {
        return orc::EncodingStrategy::EncodingStrategy_SPEED;
    } else if (strategy == "compression") {
        return orc::EncodingStrategy::EncodingStrategy_COMPRESSION;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid encoding strategy [%s].", strategy.c_str());
    }
}

std::string OrcWriterOptions::EncodingStrategyToStr(orc::EncodingStrategy strategy)
{
    if (strategy == orc::EncodingStrategy::EncodingStrategy_SPEED) {
        return "speed";
    } else if (strategy == orc::EncodingStrategy::EncodingStrategy_COMPRESSION) {
        return "compression";
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid encoding strategy kind to str.");
    }
}

orc::IntegerEncodingVersion OrcWriterOptions::StrToIntegerEncodingVersion(const std::string& encodingVersion)
{
    if (encodingVersion == "RleVersion_1") {
        return orc::IntegerEncodingVersion::RleVersion_1;
    } else if (encodingVersion == "RleVersion_2") {
        return orc::IntegerEncodingVersion::RleVersion_2;
    } else if (encodingVersion == "FastPFor128") {
        return orc::IntegerEncodingVersion::FastPFor128;
    } else if (encodingVersion == "FastPFor256") {
        return orc::IntegerEncodingVersion::FastPFor256;
    } else if (encodingVersion == "Plain") {
        return orc::IntegerEncodingVersion::Plain;
    } else if (encodingVersion == "Adaptive") {
        return orc::IntegerEncodingVersion::Adaptive;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid integer encoding version [%s].", encodingVersion.c_str());
    }
}

std::string OrcWriterOptions::IntegerEncodingVersionToStr(orc::IntegerEncodingVersion encodingVersion)
{
    if (encodingVersion == orc::IntegerEncodingVersion::RleVersion_1) {
        return "RleVersion_1";
    } else if (encodingVersion == orc::IntegerEncodingVersion::RleVersion_2) {
        return "RleVersion_2";
    } else if (encodingVersion == orc::IntegerEncodingVersion::FastPFor128) {
        return "FastPFor128";
    } else if (encodingVersion == orc::IntegerEncodingVersion::FastPFor256) {
        return "FastPFor256";
    } else if (encodingVersion == orc::IntegerEncodingVersion::Plain) {
        return "Plain";
    } else if (encodingVersion == orc::IntegerEncodingVersion::Adaptive) {
        return "Adaptive";
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid integer encoding version to str.");
    }
}

orc::BloomFilterVersion OrcWriterOptions::StrToBloomFilterVersion(const std::string& bloomFilterVersion)
{
    if (bloomFilterVersion == "ORIGIN") {
        return orc::BloomFilterVersion::ORIGINAL;
    } else if (bloomFilterVersion == "UTF8") {
        return orc::BloomFilterVersion::UTF8;
    } else if (bloomFilterVersion == "ROARING") {
        return orc::BloomFilterVersion::ROARING;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid bloom filter version [%s].", bloomFilterVersion.c_str());
    }
}

std::string OrcWriterOptions::BloomFilterVersionToStr(orc::BloomFilterVersion bloomFilterVersion)
{
    if (bloomFilterVersion == orc::BloomFilterVersion::ORIGINAL) {
        return "ORIGIN";
    } else if (bloomFilterVersion == orc::BloomFilterVersion::UTF8) {
        return "UTF8";
    } else if (bloomFilterVersion == orc::BloomFilterVersion::ROARING) {
        return "ROARING";
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "invalid bloom filter version to str.");
    }
}

void OrcWriterOptions::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
#define SET_OPTIONS(type, key, funcName)                                                                               \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(keyStr);                                                                              \
        if (iter != jsonMap.end()) {                                                                                   \
            type value;                                                                                                \
            _changedOptions.insert(key);                                                                               \
            FromJson(value, iter->second);                                                                             \
            orc::WriterOptions::funcName(value);                                                                       \
        }                                                                                                              \
    } while (0)
#define SET_ENUM_OPTIONS(type, key, funcName, castFunc)                                                                \
    do {                                                                                                               \
        std::string keyStr = key;                                                                                      \
        auto iter = jsonMap.find(key);                                                                                 \
        if (iter != jsonMap.end()) {                                                                                   \
            type value;                                                                                                \
            _changedOptions.insert(key);                                                                               \
            FromJson(value, iter->second);                                                                             \
            orc::WriterOptions::funcName(castFunc(value));                                                             \
        }                                                                                                              \
    } while (0)

#define OPTIONS_TO_JSON(key, funcName)                                                                                 \
    do {                                                                                                               \
        if (_changedOptions.find(key) != _changedOptions.end()) {                                                      \
            auto value = orc::WriterOptions::funcName();                                                               \
            json.Jsonize(key, value);                                                                                  \
        }                                                                                                              \
    } while (0)

#define ENUM_OPTIONS_TO_JSON(key, funcName, castFunc)                                                                  \
    do {                                                                                                               \
        if (_changedOptions.find(key) != _changedOptions.end()) {                                                      \
            auto value = castFunc(orc::WriterOptions::funcName());                                                     \
            json.Jsonize(key, value);                                                                                  \
        }                                                                                                              \
    } while (0)

    if (json.GetMode() == FROM_JSON) {
        auto jsonMap = json.GetMap();
        SET_OPTIONS(uint64_t, "stripe_size", setStripeSize);
        SET_OPTIONS(uint64_t, "stripe_row_count", setStripeRowCount);
        SET_OPTIONS(uint64_t, "compression_block_size", setCompressionBlockSize);
        SET_OPTIONS(uint64_t, "memory_block_size", setMemoryBlockSize);
        SET_OPTIONS(uint64_t, "row_index_stride", setRowIndexStride);
        SET_OPTIONS(bool, "block_padding", setBlockPadding);
        SET_OPTIONS(uint32_t, "compression_level", setCompressionLevel);
        SET_OPTIONS(double, "padding_tolerance", setPaddingTolerance);
        SET_OPTIONS(double, "dictionary_key_ratio_threshold", setDictionaryKeyRatioThreshold);
        SET_OPTIONS(uint64_t, "dictionary_size_threshold", setDictionarySizeThreshold);
        SET_OPTIONS(bool, "enable_stats", setEnableStats);
        SET_OPTIONS(bool, "enable_str_stats_cmp", setEnableStrStatsCmp);
        SET_OPTIONS(bool, "enable_dictionary", setEnableDictionary);
        SET_OPTIONS(bool, "sort_dictionary", setSortDictionary);
        SET_OPTIONS(std::string, "time_zone", setTimezoneName);
        SET_OPTIONS(bool, "enable_sort_stripe_streams", setEnableSortStripeStreams);
        SET_OPTIONS(bool, "align_block_bound_to_row_group", setAlignBlockToRowGroup);
        SET_OPTIONS(bool, "optimize_decimal", setOptimizeDecimal);
        SET_OPTIONS(uint32_t, "max_string_cmp_size", setMaxStringCmpSize);
        SET_OPTIONS(bool, "use_adaptive_integer_encoding", setEnableAdaptiveIntegerEncoding);
        SET_OPTIONS(bool, "enable_verify_compression", setEnableVerifyCompression);
        SET_OPTIONS(bool, "enable_verify_checksum", setEnableVerifyChecksum);
        SET_OPTIONS(bool, "add_one_sec_for_jdk_bug", setCompensateTimestampJdkBug);
        SET_ENUM_OPTIONS(std::string, "compression", setCompression, StrToCompressionKind);
        SET_ENUM_OPTIONS(std::string, "encoding_strategy", setEncodingStrategy, StrToEncodingStrategy);
        SET_ENUM_OPTIONS(std::string, "bloom_filter_version", setBloomFilterVersion, StrToBloomFilterVersion);
        SET_ENUM_OPTIONS(std::string, "int_encoding_version", setIntegerEncodingVersion, StrToIntegerEncodingVersion);
    } else {
        OPTIONS_TO_JSON("stripe_size", getStripeSize);
        OPTIONS_TO_JSON("stripe_row_count", getStripeRowCount);
        OPTIONS_TO_JSON("compression_block_size", getCompressionBlockSize);
        OPTIONS_TO_JSON("memory_block_size", getMemoryBlockSize);
        OPTIONS_TO_JSON("row_index_stride", getRowIndexStride);
        OPTIONS_TO_JSON("block_padding", getBlockPadding);
        OPTIONS_TO_JSON("compression_level", getCompressionLevel);
        OPTIONS_TO_JSON("padding_tolerance", getPaddingTolerance);
        OPTIONS_TO_JSON("dictionary_key_ratio_threshold", getDictionaryKeyRatioThreshold);
        OPTIONS_TO_JSON("dictionary_size_threshold", getDictionarySizeThreshold);
        OPTIONS_TO_JSON("enable_stats", getEnableStats);
        OPTIONS_TO_JSON("enable_str_stats_cmp", getEnableStrStatsCmp);
        OPTIONS_TO_JSON("enable_dictionary", getEnableDictionary);
        OPTIONS_TO_JSON("sort_dictionary", getSortDictionary);
        OPTIONS_TO_JSON("time_zone", getTimezoneName);
        OPTIONS_TO_JSON("enable_sort_stripe_streams", getEnableSortStripeStreams);
        OPTIONS_TO_JSON("align_block_bound_to_row_group", getAlignBlockToRowGroup);
        OPTIONS_TO_JSON("optimize_decimal", getOptimizeDecimal);
        OPTIONS_TO_JSON("max_string_cmp_size", getMaxStringCmpSize);
        OPTIONS_TO_JSON("use_adaptive_integer_encoding", getEnableAdaptiveIntegerEncoding);
        OPTIONS_TO_JSON("enable_verify_compression", getEnableVerifyCompression);
        OPTIONS_TO_JSON("enable_verify_checksum", getEnableVerifyChecksum);
        OPTIONS_TO_JSON("add_one_sec_for_jdk_bug", getCompensateTimestampJdkBug);
        ENUM_OPTIONS_TO_JSON("compression", getCompression, CompressionKindToStr);
        ENUM_OPTIONS_TO_JSON("encoding_strategy", getEncodingStrategy, EncodingStrategyToStr);
        ENUM_OPTIONS_TO_JSON("bloom_filter_version", getBloomFilterVersion, BloomFilterVersionToStr);
        ENUM_OPTIONS_TO_JSON("int_encoding_version", getIntegerEncodingVersion, IntegerEncodingVersionToStr);
    }
} // namespace indexlibv2::config

} // namespace indexlibv2::config
