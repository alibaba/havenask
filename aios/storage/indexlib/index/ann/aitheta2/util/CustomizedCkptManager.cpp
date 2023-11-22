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
#include "indexlib/index/ann/aitheta2/util/CustomizedCkptManager.h"

#include <regex>

#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaDumper.h"

using namespace std;
namespace aitheta2 {

int CustomizedCkptManager::init(const IndexParams& params, const std::string& ckptDirName)
{
    _params = params;
    _params.get(DISABLE_CLEANUP, &_disableCleanup);
    _ckptDirName = ckptDirName;
    return 0;
}

bool CustomizedCkptManager::MakeCkptDirectory()
{
    if (_ckptDirectory) {
        return true;
    }
    _ckptDirectory = _rootDirectory->MakeDirectory(_ckptDirName);
    if (_ckptDirectory) {
        return true;
    }
    LOG_ERROR("create[%s] in directory[%s] failed", _ckptDirName.c_str(), _ckptDirectory->DebugString().c_str());
    return false;
}

//! Create ckpt dumper to save index builder's ckpt
IndexDumper::Pointer CustomizedCkptManager::create_ckpt_dumper(const std::string& ckptId)
{
    if (ckptId.empty()) {
        LOG_ERROR("ckpt id is empty");
        return IndexDumper::Pointer();
    }

    if (_ckptFileWriters.find(ckptId) != _ckptFileWriters.end()) {
        LOG_ERROR("dumper with ckptId[%s] already created", ckptId.c_str());
        return IndexDumper::Pointer();
    }

    if (!MakeCkptDirectory()) {
        return IndexDumper::Pointer();
    }

    if (_ckptDirectory->IsExist(ckptId)) {
        LOG_ERROR("ckpt file[%s] exists in directory[%s]", ckptId.c_str(), _ckptDirectory->DebugString().c_str());
        return IndexDumper::Pointer();
    }
    auto ckptFileWriter = _ckptDirectory->CreateFileWriter(ckptId);
    if (!ckptFileWriter) {
        LOG_ERROR("Create file[%s] failed in directory[%s]", ckptId.c_str(), _ckptDirectory->DebugString().c_str());
        return IndexDumper::Pointer();
    }

    _ckptFileWriters[ckptId] = ckptFileWriter;
    auto ckptWriter = std::make_shared<indexlibv2::index::ann::IndexDataWriter>(ckptFileWriter);

    auto dumper = CustomizedAiThetaDumper::create(ckptWriter);
    if (dumper) {
        LOG_INFO("get ckpt[%s] dumper success", ckptFileWriter->DebugString().c_str());
    }
    return dumper;
}

//! Get ckpt container to recover index builder
IndexContainer::Pointer CustomizedCkptManager::get_ckpt_container(const std::string& ckptId)
{
    if (ckptId.empty()) {
        LOG_ERROR("ckpt id is empty");
        return IndexContainer::Pointer();
    }

    if (!MakeCkptDirectory()) {
        return IndexContainer::Pointer();
    }

    std::string flagFile = ckptId + DONE_FILE_SUFFIX;
    if (!_ckptDirectory->IsExist(flagFile)) {
        LOG_ERROR("file[%s] not exist", flagFile.c_str());
        return IndexContainer::Pointer();
    }

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_BUFFERED);
    auto ckptFileReader = _ckptDirectory->CreateFileReader(ckptId, readerOption);
    if (!ckptFileReader) {
        LOG_ERROR("create file reader[%s] failed in directory[%s]", ckptId.c_str(),
                  _ckptDirectory->DebugString().c_str());
        return IndexContainer::Pointer();
    }

    auto ckptReader = std::make_shared<indexlibv2::index::ann::IndexDataReader>(ckptFileReader);
    auto container = std::make_shared<aitheta2::CustomizedAiThetaContainer>(ckptReader);
    int error = container->init(_params);
    if (error != 0) {
        LOG_ERROR("container init failed");
        return IndexContainer::Pointer();
    }
    error = container->load();
    if (error != 0) {
        LOG_ERROR("container load failed");
        return IndexContainer::Pointer();
    }
    LOG_INFO("get ckpt[%s] container done", ckptFileReader->DebugString().c_str());
    return container;
}

int CustomizedCkptManager::dump_ckpt_finish(const std::string& ckptId)
{
    auto iterator = _ckptFileWriters.find(ckptId);
    if (iterator == _ckptFileWriters.end()) {
        LOG_ERROR("ckpt[%s] dumper not created", ckptId.c_str());
        return aitheta2::IndexError_NoExist;
    }
    auto writer = iterator->second;
    auto status = writer->Close();
    if (!status.OK()) {
        LOG_ERROR("close ckptId[%s] with ckpt file writer[%s] failed, err[%s]", ckptId.c_str(),
                  writer->DebugString().c_str(), status.Status().ToString().c_str());
        return aitheta2::IndexError_IO;
    }
    _ckptFileWriters.erase(iterator);

    if (!MakeDoneFlagFile(ckptId)) {
        return aitheta2::IndexError_IO;
    }

    return 0;
}

std::vector<std::string> CustomizedCkptManager::get_ckpt_ids()
{
    std::vector<std::string> ckptIds;
    if (!MakeCkptDirectory()) {
        return ckptIds;
    }

    std::vector<std::string> fileNames;
    _ckptDirectory->ListDir("", fileNames);
    for (const auto& fname : fileNames) {
        if (!std::regex_match(fname, std::regex(DONE_FILE_PATTERN))) {
            continue;
        }
        std::string ckptId = fname.substr(0, fname.size() - DONE_FILE_SUFFIX.size());
        if (!_ckptDirectory->IsExist(ckptId)) {
            LOG_WARN("ckpt[%s] file not exists in direcory[%s], skip", ckptId.c_str(),
                     _ckptDirectory->DebugString().c_str());
            continue;
        }
        ckptIds.push_back(ckptId);
    }
    LOG_INFO("valid ckpts is[%s]", autil::StringUtil::toString(ckptIds, ",").c_str());
    return ckptIds;
}

//! Cleanup all ckpts
int CustomizedCkptManager::cleanup(void)
{
    if (_ckptDirectory == nullptr) {
        return 0;
    }

    int ret = 0;
    for (auto& [ckptId, fileWriter] : _ckptFileWriters) {
        LOG_ERROR("ckpt[%s] with writer[%s] not call dump_ckpt_finish", ckptId.c_str(),
                  fileWriter->DebugString().c_str());
        ret = aitheta2::IndexError_IO;
        auto status = fileWriter->Close();
        if (!status.OK()) {
            LOG_ERROR("close ckptId[%s] with ckpt file writer[%s] failed, err[%s]", ckptId.c_str(),
                      fileWriter->DebugString().c_str(), status.Status().ToString().c_str());
        }
    }
    _ckptFileWriters.clear();

    // for test
    if (_disableCleanup) {
        LOG_WARN("disable cleanup, not remove files in directory[%s]", _ckptDirectory->DebugString().c_str());
        return ret;
    }

    // use try-catch to take care of bs backup strategy
    try {
        std::vector<std::string> fileNames;
        _ckptDirectory->ListDir("", fileNames);
        for (const auto& fileName : fileNames) {
            try {
                _ckptDirectory->RemoveFile(fileName);
            } catch (const indexlib::util::ExceptionBase& e) {
                LOG_WARN("remove file[%s] failed with exception[%s] with ckpt directory[%s]", fileName.c_str(),
                         e.what(), _ckptDirectory->DebugString().c_str());
            }
        }
        _ckptDirectory.reset();
        _rootDirectory->RemoveDirectory(_ckptDirName);
        _rootDirectory.reset();
    } catch (const indexlib::util::ExceptionBase& e) {
        LOG_WARN("clean up failed with exception[%s] with ckpt directory[%s]", e.what(),
                 _ckptDirectory->DebugString().c_str());
        ret = aitheta2::IndexError_IO;
    }
    return ret;
}

bool CustomizedCkptManager::MakeDoneFlagFile(const std::string& ckptId)
{
    std::string file = ckptId + DONE_FILE_SUFFIX;
    if (_ckptDirectory->IsExist(file)) {
        LOG_ERROR("file[%s] exists in directory[%s]", file.c_str(), _ckptDirectory->DebugString().c_str());
        return false;
    }
    auto fileWriter = _ckptDirectory->CreateFileWriter(file);
    if (!fileWriter) {
        LOG_ERROR("create file[%s] failed in directory[%s]", file.c_str(), _ckptDirectory->DebugString().c_str());
        return false;
    }
    size_t ts = autil::TimeUtility::currentTimeInSeconds();
    fileWriter->Write(&ts, sizeof(ts)).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    return true;
}

} // namespace aitheta2