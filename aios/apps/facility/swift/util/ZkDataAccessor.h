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

#include <stdint.h>
#include <string>
#include <zlib.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/Log.h"
#include "autil/ZlibCompressor.h"
#include "swift/common/Common.h"
#include "swift/util/TargetRecorder.h"
#include "zookeeper/zookeeper.h"

namespace swift {
namespace util {
struct ZkFileHeader {
    int64_t magicNum;
    ;
    int32_t Flags;
};

class ZkDataAccessor {
public:
    ZkDataAccessor();
    virtual ~ZkDataAccessor();

private:
    ZkDataAccessor(const ZkDataAccessor &);
    ZkDataAccessor &operator=(const ZkDataAccessor &);

public:
    virtual bool init(cm_basic::ZkWrapper *zkWrapper, const std::string &sysRoot);
    virtual bool init(const std::string &sysRoot);
    virtual bool remove(const std::string &path);
    cm_basic::ZkWrapper *getZkWrapper();
    bool getData(const std::string &path, std::string &content);

    template <typename ProtoClass>
    bool readZk(const std::string &path, ProtoClass &protoValue, bool compress = false, bool json = false) const;
    template <typename ProtoClass>
    bool writeZk(const std::string &path, const ProtoClass &protoValue, bool compress = false, bool json = false);
    bool writeFile(const std::string &path, const std::string &content);
    bool createPath(const std::string &path);

protected:
    bool _ownZkWrapper = false;
    cm_basic::ZkWrapper *_zkWrapper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ZkDataAccessor);
/////////////////////////////////////////

template <typename ProtoClass>
bool ZkDataAccessor::readZk(const std::string &path, ProtoClass &protoValue, bool compress, bool json) const {
    std::string content;
    ZOO_ERRORS ec = _zkWrapper->getData(path, content);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "fail to get data %s", path.c_str());
        return false;
    }
    if (compress) {
        autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
        std::string decompressedData;
        if (!compressor.decompress(content, decompressedData)) {
            AUTIL_LOG(ERROR, "decompress failed");
            return false;
        }
        content = decompressedData;
    }
    if (json) {
        if (!http_arpc::ProtoJsonizer::fromJsonString(content, &protoValue)) {
            AUTIL_LOG(ERROR, "fail to parse form json %s", content.c_str());
            return false;
        }
    } else {
        if (!protoValue.ParseFromString(content)) {
            util::TargetRecorder::newRecord(content, "file_failed", "failed_");
            AUTIL_LOG(ERROR, "fail to parse form string, size [%lu]", content.size());
            protoValue.Clear();
            return false;
        }
    }
    return true;
}

template <typename ProtoClass>
bool ZkDataAccessor::writeZk(const std::string &path, const ProtoClass &protoValue, bool compress, bool json) {
    std::string content;
    if (json) {
        content = http_arpc::ProtoJsonizer::toJsonString(protoValue);
    } else {
        protoValue.SerializeToString(&content);
    }
    if (compress) {
        autil::ZlibCompressor compressor(Z_BEST_COMPRESSION);
        std::string compressedData;
        if (!compressor.compress(content, compressedData)) {
            AUTIL_LOG(ERROR, "compress %s failed", protoValue.ShortDebugString().c_str());
            return false;
        }
        content = compressedData;
    }
    if (!writeFile(path, content)) {
        AUTIL_LOG(ERROR, "fail to write data %s", path.c_str());
        return false;
    }
    return true;
}

} // namespace util
} // namespace swift
