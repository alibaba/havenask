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

#include "suez/drc/LogRewriter.h"

namespace indexlibv2::framework {
class ITablet;
class ITabletReader;
} // namespace indexlibv2::framework

namespace indexlibv2::index {
class KVIndexReader;
}

namespace suez {
class BinaryRecordDecoder;

class KVUpdate2Add : public LogRewriter {
public:
    KVUpdate2Add();
    ~KVUpdate2Add();

public:
    bool init(indexlibv2::framework::ITablet *index) override;
    RewriteCode rewrite(LogRecord &log) override;
    bool createSnapshot() override;
    void releaseSnapshot() override;

private:
    std::unique_ptr<BinaryRecordDecoder>
    prepareDecoder(const std::shared_ptr<indexlibv2::framework::ITabletReader> &reader) const;

private:
    indexlibv2::framework::ITablet *_index = nullptr;
    std::unique_ptr<BinaryRecordDecoder> _decoder;
    std::string _indexFieldName;
    std::shared_ptr<indexlibv2::index::KVIndexReader> _readerSnapshot;
};

} // namespace suez
