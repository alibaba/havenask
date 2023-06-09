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
#include <set>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "orc/Common.hh"
#include "orc/Writer.hh"

namespace indexlibv2::config {

class OrcWriterOptions : public orc::WriterOptions, public autil::legacy::Jsonizable
{
public:
    OrcWriterOptions();
    ~OrcWriterOptions();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    static orc::CompressionKind StrToCompressionKind(const std::string& compression);
    static orc::EncodingStrategy StrToEncodingStrategy(const std::string& encodingStrategy);
    static orc::IntegerEncodingVersion StrToIntegerEncodingVersion(const std::string& encodingVersion);
    static orc::BloomFilterVersion StrToBloomFilterVersion(const std::string& bloomFilterVersion);

    static std::string CompressionKindToStr(orc::CompressionKind compression);
    static std::string EncodingStrategyToStr(orc::EncodingStrategy strategy);
    static std::string IntegerEncodingVersionToStr(orc::IntegerEncodingVersion encodingVersion);
    static std::string BloomFilterVersionToStr(orc::BloomFilterVersion bloomFilterVersion);

    bool NeedToJson() const { return _changedOptions.size() != 0; }

private:
    std::set<std::string> _changedOptions;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
