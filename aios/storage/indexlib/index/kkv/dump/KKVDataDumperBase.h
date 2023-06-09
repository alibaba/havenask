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

#include "indexlib/index/kkv/common/KKVDoc.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDocSorterBase.h"
#include "indexlib/index/kkv/dump/KKVDumpPhrase.h"
#include "indexlib/index/kkv/dump/PKeyDumper.h"
namespace indexlibv2::index {

enum class KKVDumpPhrase;

class KKVDataDumperBase
{
public:
    KKVDataDumperBase(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, bool storeTs, KKVDumpPhrase phrase)
        : _kkvIndexConfig(kkvIndexConfig)
        , _storeTs(storeTs)
        , _phrase(phrase)
        , _curPkey(0)
    {
    }

    virtual ~KKVDataDumperBase() = default;

public:
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::file_system::WriterOption& skeyOption,
                const indexlib::file_system::WriterOption& valueOption, size_t totalPkeyCount);
    Status Dump(uint64_t pkey, bool pkeyDeleted, bool isLastSkey, const KKVDoc& kkvDoc);
    Status Close();

public:
    size_t GetPKeyCount() const { return _pkeyDumper->GetPKeyCount(); }
    virtual size_t GetMaxValueLen() const = 0;
    virtual size_t GetTotalSKeyCount() const = 0;
    virtual uint32_t GetMaxSKeyCount() const = 0;
    virtual bool IsValueInline() const = 0;

protected:
    virtual Status InitSkeyAndValueDumper(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                          const indexlib::file_system::WriterOption& skeyOption,
                                          const indexlib::file_system::WriterOption& valueOption) = 0;
    virtual Status CloseSkeyAndValueDumper() = 0;
    virtual Status DoDump(uint64_t pkey, bool pkeyDeleted, bool isLastSkey, const KKVDoc& doc) = 0;

protected:
    Status SortDump(uint64_t pkey, bool pkeyDeleted, bool isLastSkey, const KKVDoc& doc);
    Status DumpMeta();

protected:
    std::shared_ptr<config::KKVIndexConfig> _kkvIndexConfig;
    bool _storeTs;
    KKVDumpPhrase _phrase;

    std::shared_ptr<indexlib::file_system::IDirectory> _dumpDirectory;
    std::unique_ptr<PKeyDumper> _pkeyDumper;
    std::unique_ptr<KKVDocSorterBase> _kkvDocSorter;

    uint64_t _curPkey;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
