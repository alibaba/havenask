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
#include "build_service/config/DocReclaimSource.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, DocReclaimSource);

string DocReclaimSource::DOC_RECLAIM_SOURCE = "doc_reclaim_source";

DocReclaimSource::DocReclaimSource() : msgTTLInSeconds(-1) {}

DocReclaimSource::~DocReclaimSource() {}

void DocReclaimSource::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("swift_root", swiftRoot);
    json.Jsonize("topic_name", topicName);
    json.Jsonize("swift_client_config", clientConfigStr, clientConfigStr);
    json.Jsonize("swift_reader_config", readerConfigStr, readerConfigStr);
    json.Jsonize("ttl_in_seconds", msgTTLInSeconds, msgTTLInSeconds);
}

}} // namespace build_service::config
