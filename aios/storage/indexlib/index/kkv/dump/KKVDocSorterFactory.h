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

#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDumpPhrase.h"
#include "indexlib/index/kkv/dump/NormalKKVDocSorter.h"
#include "indexlib/index/kkv/dump/TruncateKKVDocSorter.h"

namespace indexlibv2::index {

class KKVDocSorterFactory
{
public:
    KKVDocSorterFactory() = delete;
    ~KKVDocSorterFactory() = delete;

public:
    static std::pair<Status, std::unique_ptr<KKVDocSorterBase>>
    CreateIfNeccessary(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, KKVDumpPhrase phrase)
    {
        assert(kkvIndexConfig);
        auto sorter = DoCreateIfNeccessary(kkvIndexConfig, phrase);
        if (sorter != nullptr && !sorter->Init()) {
            return {Status::InvalidArgs("init kkv doc sorter failed"), nullptr};
        }
        return {Status::OK(), std::move(sorter)};
    }

private:
    static std::unique_ptr<KKVDocSorterBase>
    DoCreateIfNeccessary(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, KKVDumpPhrase phrase)
    {
        bool needSort = false;
        bool needTruncate = false;
        switch (phrase) {
        case KKVDumpPhrase::RT_BUILD_DUMP:
            if (kkvIndexConfig->NeedSuffixKeyTruncate()) {
                needTruncate = true;
            }
            break;
        case KKVDumpPhrase::MERGE_BOTTOMLEVEL:
            if (kkvIndexConfig->EnableSuffixKeyKeepSortSequence()) {
                needSort = true;
            } else if (kkvIndexConfig->NeedSuffixKeyTruncate()) {
                needTruncate = true;
            }
            break;
        case KKVDumpPhrase::MERGE:
            needTruncate = true;
            break;
        default:
            assert(phrase == KKVDumpPhrase::BUILD_DUMP);
            break;
        }

        if (needTruncate) {
            return std::make_unique<TruncateKKVDocSorter>(kkvIndexConfig);
        } else if (needSort) {
            return std::make_unique<NormalKKVDocSorter>(kkvIndexConfig);
        } else {
            return nullptr;
        }
    }
};

} // namespace indexlibv2::index
