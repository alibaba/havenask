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
#include "indexlib/index/kkv/dump/InlineSKeyDumper.h"
#include "indexlib/index/kkv/dump/KKVDataDumperBase.h"

namespace indexlibv2::index {

template <typename SKeyType>
class InlineKKVDataDumper final : public KKVDataDumperBase
{
public:
    InlineKKVDataDumper(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, bool storeTs,
                        KKVDumpPhrase phrase)
        : KKVDataDumperBase(kkvIndexConfig, storeTs, phrase)
    {
    }
    ~InlineKKVDataDumper() = default;

public:
    size_t GetMaxValueLen() const override
    {
        assert(_skeyDumper);
        return _skeyDumper->GetMaxValueLen();
    }

    size_t GetTotalSKeyCount() const override
    {
        assert(_skeyDumper);
        return _skeyDumper->GetTotalSKeyCount();
    }

    uint32_t GetMaxSKeyCount() const override
    {
        assert(_skeyDumper);
        return _skeyDumper->GetMaxSKeyCount();
    }

    bool IsValueInline() const override { return true; }

protected:
    Status InitSkeyAndValueDumper(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                  const indexlib::file_system::WriterOption& skeyOption,
                                  const indexlib::file_system::WriterOption&) override
    {
        assert(directory);
        auto valueConfig = _kkvIndexConfig->GetValueConfig();
        assert(valueConfig);
        _skeyDumper = std::make_unique<InlineSKeyDumper<SKeyType>>(_storeTs, _kkvIndexConfig->StoreExpireTime(),
                                                                   valueConfig->GetFixedLength());
        return _skeyDumper->Init(directory, skeyOption);
    }

    Status CloseSkeyAndValueDumper() override
    {
        assert(_skeyDumper);
        return _skeyDumper->Close();
    }

    Status DoDump(uint64_t pkey, bool isPkeyDeleted, bool isLastSkey, const KKVDoc& doc) override
    {
        assert(_skeyDumper);
        assert(_pkeyDumper);

        auto [status, skeyOffset] = _skeyDumper->Dump(isPkeyDeleted, doc.skeyDeleted, isLastSkey, doc.skey,
                                                      doc.timestamp, doc.expireTime, doc.value);
        RETURN_IF_STATUS_ERROR(status, "dump value and skey failed");
        RETURN_IF_STATUS_ERROR(_pkeyDumper->Dump(pkey, skeyOffset), "dump pkey failed");
        return Status::OK();
    }

private:
    std::unique_ptr<InlineSKeyDumper<SKeyType>> _skeyDumper;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, InlineKKVDataDumper, SKeyType);

} // namespace indexlibv2::index