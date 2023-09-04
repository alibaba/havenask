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

#include <functional>
#include <memory>
#include <string>

#include "autil/Lock.h"
#include "autil/NoCopyable.h"
#include "autil/result/Result.h"
#include "suez/sdk/WriteResult.h"

namespace build_service::config {
class ResourceReader;
}

namespace build_service::proto {
class PartitionId;
}

namespace build_service::util {
class SwiftClientCreator;
}

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace indexlib::document {
class RawDocumentParser;
class DocumentFactoryWrapper;
} // namespace indexlib::document

namespace indexlibv2::framework {
class ITablet;
}

namespace suez {

class WALConfig;
class WALStrategy;

using WalDoc = std::pair<uint16_t, std::string>;
using WalDocVector = std::vector<WalDoc>;

struct WriteMetricsCollector;

class TableWriter : autil::NoCopyable {
public:
    TableWriter();
    virtual ~TableWriter();

public:
    bool init(const build_service::proto::PartitionId &pid,
              const std::string &configPath, // for document parser
              const kmonitor::MetricsReporterPtr &reporter,
              const WALConfig &walConfig,
              const std::shared_ptr<build_service::util::SwiftClientCreator> &swiftClientCreator);

    void setEnableWrite(bool flag);

    void write(const std::string &format,
               const WalDocVector &docs,
               const std::function<void(autil::Result<WriteResult>)> &done);

    void
    updateSchema(uint32_t version, const std::string &configPath, std::function<void(autil::Result<int64_t>)> done);

    void stop();

    void updateIndex(const std::shared_ptr<indexlibv2::framework::ITablet> &index);
    std::shared_ptr<indexlibv2::framework::ITablet> getIndex() const;
    void setRoleType(const std::string &roleType) { _roleType = roleType; }

private:
    void maybeInitWALLocked();
    autil::Result<WalDocVector> parseWalDocs(const std::string &format, const WalDocVector &docs);
    std::unique_ptr<indexlib::document::RawDocumentParser> getParser(const std::string &format);

    void updateLatestLogOffset(int64_t latestLogOffset);
    int64_t getLatestLogOffset() const;

    void fillWriteResult(WriteResult &result, const WriteMetricsCollector *collector) const;

private:
    std::unique_ptr<build_service::proto::PartitionId> _pid;
    kmonitor::MetricsReporterPtr _reporter;
    std::unique_ptr<WALConfig> _walConfig;
    std::shared_ptr<build_service::util::SwiftClientCreator> _swiftClientCreator;
    std::shared_ptr<indexlib::document::DocumentFactoryWrapper> _documentFactoryWrapper;

    std::atomic<bool> _enableWrite = false;
    std::unique_ptr<WALStrategy> _wal;
    mutable autil::ThreadMutex _mutex;
    std::atomic<int64_t> _latestLogOffset = -1;

    // index: for watermark and sync write
    std::shared_ptr<indexlibv2::framework::ITablet> _index;
    mutable autil::ReadWriteLock _indexLock;
    // for report metric
    std::string _roleType;
};

} // namespace suez
