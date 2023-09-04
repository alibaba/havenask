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
#include "master/CompressedSerializer.h"
#include "carbon/Log.h"
#include "autil/ZlibCompressor.h"
#include "autil/LzmaCompressor.h"
#include "autil/EnvUtil.h"
#include "monitor/MonitorUtil.h"

using namespace std;
using namespace hippo;
using namespace autil;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, CompressedSerializer);

CompressedSerializer::CompressedSerializer(LeaderSerializer *leaderSerializer,
        const string &id)
    : Serializer(leaderSerializer, id)
{
}

CompressedSerializer::~CompressedSerializer() {
}

bool CompressedSerializer::read(string &content) const {
    string str;
    {
        REPORT_USED_TIME(_id, METRIC_READ_ZK_TIME);
        if (!Serializer::read(str)) {
            return false;
        }
    }
    REPORT_METRIC(_id, METRIC_READ_ZK_FILE_SIZE,
                  (double)str.size()/1024.0);

    return decompress(str, content);
}

bool CompressedSerializer::write(const string &content) {
    string str;
    if (!compress(content, str)) {
        return false;
    }
    REPORT_METRIC(_id, METRIC_WRITE_ZK_FILE_SIZE,
                  (double)str.size()/1024.0);

    {
        REPORT_USED_TIME(_id, METRIC_WRITE_ZK_TIME);
        return Serializer::write(str);
    }
}

bool CompressedSerializer::compress(const string &in, string &out) const {
    REPORT_USED_TIME(_id, METRIC_COMPRESS_TIME);
    uint32_t preset = autil::EnvUtil::getEnv("CARBON_LZMA_PRESET", 1);
    LzmaCompressor compressor(preset);
    if (!compressor.compress(in, out)) {
        CARBON_LOG(ERROR, "compress content failed.");
        return false;
    }
    return true;
}

bool CompressedSerializer::decompress(const string &in, string &out) const {
    REPORT_USED_TIME(_id, METRIC_DECOMPRESS_TIME);
    LzmaCompressor lzmaCompressor;
    ZlibCompressor zlibCompressor;
    if (lzmaCompressor.decompress(in, out)) {
        return true;
    }
    CARBON_LOG(ERROR, "decompress content with lzma mode failed, "
               "try zlib mode.");
    if (!zlibCompressor.decompress(in, out)) {
        CARBON_LOG(ERROR, "decompress content with zlib mode failed.");
        return false;
    }
    return true;
}


END_CARBON_NAMESPACE(master);

