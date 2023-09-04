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
#include "sql/resource/SwiftMessageWriter.h"

#include <functional>
#include <iosfwd>
#include <limits>
#include <stdint.h>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "autil/result/Result.h"
#include "suez/sdk/RemoteTableWriterParam.h"
#include "swift/client/helper/CheckpointBuffer.h"
#include "swift/common/MessageInfo.h"

using namespace std;
using namespace autil;
using namespace autil::result;
using namespace swift::client;

namespace sql {

SwiftMessageWriter::SwiftMessageWriter(SwiftWriterAsyncHelperPtr swiftWriter) {
    _swiftWriter = std::move(swiftWriter);
}

SwiftMessageWriter::~SwiftMessageWriter() {}

void SwiftMessageWriter::write(suez::MessageWriteParam param) {
    if (_swiftWriter == nullptr) {
        param.cb(RuntimeError::make("swift message writer is empty!"));
        return;
    }
    vector<swift::common::MessageInfo> messageInfos;
    for (const auto &msg : param.docs) {
        messageInfos.emplace_back(swift::common::MessageInfo(msg.second, 0, msg.first, 0));
    }

    _swiftWriter->write(
        messageInfos.data(),
        messageInfos.size(),
        [_cb = std::move(param.cb)](autil::result::Result<SwiftWriteCallbackParam> result) {
            if (result.is_err()) {
                _cb(std::move(result).steal_error());
            } else {
                auto &param = result.get();
                int64_t cp = -1;
                if (param.tsCount > 0 && param.tsData != nullptr) {
                    cp = param.tsData[param.tsCount - 1];
                }
                suez::WriteCallbackParam cbParam;
                cbParam.maxCp = cp;
                cbParam.maxBuildGap = std::numeric_limits<int64_t>::max();
                _cb(std::move(cbParam));
            }
        },
        param.timeoutUs);
}

} // namespace sql
