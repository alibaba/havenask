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
#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, ProtoUtil);

using google::protobuf::Message;
using google::protobuf::util::MessageDifferencer;

bool ProtoUtil::compareProto(const Message &expected, const Message &actual, bool strict, std::string *message) {
    MessageDifferencer md;
    if (!strict) {
        md.set_message_field_comparison(MessageDifferencer::EQUIVALENT);
    }
    if (message != nullptr) {
        md.ReportDifferencesToString(message);
    }
    return md.Compare(expected, actual);
}

bool ProtoUtil::compareProto(const Message &expected, const Message &actual, DiffCustomizer customizer) {
    MessageDifferencer md;
    customizer(md);
    return md.Compare(expected, actual);
}

} // namespace catalog
