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
#include "build_service/workflow/RawDocChecksumer.h"

#include "autil/HashAlgorithm.h"
#include "fslib/util/FileUtil.h"

using namespace std;

using namespace build_service::document;
namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, RawDocChecksumer);

RawDocChecksumer::RawDocChecksumer(const proto::PartitionId& pid, const config::ResourceReaderPtr& resourceReader)
    : _pid(pid)
    , _resourceReader(resourceReader)
{
}

bool RawDocChecksumer::ensureInited()
{
    if (_inited) {
        return true;
    }
    if (!_resourceReader->getDataTableConfigWithJsonPath(
            _pid.buildid().datatable(), "workflow_config.DocReaderProducer.enable_checksum", _enabled)) {
        AUTIL_LOG(ERROR, "read workflow_config.DocReaderProducer.enable_checksum failed");
        return false;
    }
    if (_enabled) {
        if (!_resourceReader->getDataTableConfigWithJsonPath(_pid.buildid().datatable(),
                                                             "workflow_config.DocReaderProducer.checksum_output_path",
                                                             _checksumOutputFile)) {
            AUTIL_LOG(ERROR, "reade workflow_config.DocReaderProducer.checksum_output_path failed");
            return false;
        }
    }
    std::stringstream ss;
    ss << "/" << _pid.buildid().datatable() << "_" << _pid.buildid().generationid() << "_" << _pid.range().from() << "_"
       << _pid.range().to();
    _checksumOutputFile += ss.str();
    _inited = true;
    AUTIL_LOG(INFO, "raw doc checksum [%s], outputPath [%s]", _enabled ? "true" : "false", _checksumOutputFile.c_str());
    return true;
}

void RawDocChecksumer::recover(uint64_t checksum)
{
    _checksum = checksum;
    AUTIL_LOG(INFO, "recover checksum [%lu]", checksum);
}
void RawDocChecksumer::evaluate(document::RawDocumentPtr& rawDoc)
{
    if (!_enabled) {
        return;
    }
    std::string rawDocStr = rawDoc->toString();
    uint64_t docHash = autil::HashAlgorithm::hashString64(rawDocStr.c_str(), rawDocStr.length());
    _checksum ^= docHash;
}
bool RawDocChecksumer::finish()
{
    if (!_enabled) {
        return true;
    }
    if (!fslib::util::FileUtil::removeIfExist(_checksumOutputFile)) {
        AUTIL_LOG(ERROR, "remove [%s] failed", _checksumOutputFile.c_str());
        return false;
    }
    if (!fslib::util::FileUtil::writeFile(_checksumOutputFile, std::to_string(_checksum))) {
        AUTIL_LOG(ERROR, "write checksum [%lu] to file [%s] failed", _checksum, _checksumOutputFile.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "write checksum [%lu] to file [%s]", _checksum, _checksumOutputFile.c_str());
    return true;
}

}} // namespace build_service::workflow
