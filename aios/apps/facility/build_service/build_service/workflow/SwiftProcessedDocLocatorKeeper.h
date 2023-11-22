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

#include "build_service/common/Locator.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftMessageFilter.h"
#include "swift/client/SwiftReader.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace build_service { namespace workflow {

class SwiftProcessedDocLocatorKeeper
{
public:
    SwiftProcessedDocLocatorKeeper();
    ~SwiftProcessedDocLocatorKeeper();

    SwiftProcessedDocLocatorKeeper(const SwiftProcessedDocLocatorKeeper&) = delete;
    SwiftProcessedDocLocatorKeeper& operator=(const SwiftProcessedDocLocatorKeeper&) = delete;
    SwiftProcessedDocLocatorKeeper(SwiftProcessedDocLocatorKeeper&&) = delete;
    SwiftProcessedDocLocatorKeeper& operator=(SwiftProcessedDocLocatorKeeper&&) = delete;

public:
    void init(uint64_t sourceSignature, const common::SwiftParam& param, int64_t startTimestamp);

    bool setSeekLocator(const common::Locator& seekLocator);
    swift::protocol::ReaderProgress getSeekProgress();
    common::Locator getSeekLocator() { return _seekLocator; }
    bool needSkip(const indexlibv2::framework::Locator::DocInfo& docInfo);
    bool update(const swift::protocol::ReaderProgress& swiftProgress,
                const indexlibv2::framework::Locator::DocInfo& docInfo);
    void updateSchemaChangeDocInfo(const indexlibv2::framework::Locator::DocInfo& docInfo);
    common::Locator getLocator() { return _currentLocator; }

private:
    indexlibv2::base::ProgressVector getSubProgress(const indexlibv2::base::MultiProgress& multiProgress,
                                                    uint32_t sourceIdx, uint32_t from, uint32_t to);

private:
    uint64_t _sourceSignature = 0;
    int64_t _startTimestamp = -1;
    common::SwiftParam _swiftParam;
    common::Locator _seekLocator;
    common::Locator _currentLocator;
    util::SwiftMessageFilter _swiftMessageFilter;
    int64_t _minTimestamp = -1;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocLocatorKeeper);

}} // namespace build_service::workflow
