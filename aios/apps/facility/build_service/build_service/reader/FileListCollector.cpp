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
#include "build_service/reader/FileListCollector.h"

#include <algorithm>

#include "autil/StringUtil.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/reader/FilePattern.h"
#include "build_service/reader/FilePatternParser.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, FileListCollector);

struct RangeIdComp {
    bool operator()(const CollectResult& left, const CollectResult& right) const
    {
        return left._rangeId < right._rangeId;
    }
};

const string FileListCollector::RAW_DOCUMENT_PATH_SEPERATOR = ",";

FileListCollector::FileListCollector() {}

FileListCollector::~FileListCollector() {}

bool FileListCollector::collect(const KeyValueMap& kvMap, const proto::Range& range, CollectResults& result)
{
    result.clear();
    CollectorDescription description;
    if (!kvMap2Description(kvMap, description)) {
        string errorMsg = "get CollectorDescription failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    description.rangeFrom = range.from();
    description.rangeTo = range.to();
    return collect(kvMap, description, result);
}

bool FileListCollector::kvMap2Description(const KeyValueMap& kvMap, CollectorDescription& description)
{
    string rawDocumentDir = getValueFromKeyValueMap(kvMap, DATA_PATH);
    if (!parseRawDocumentPaths(description.paths, rawDocumentDir)) {
        return false;
    }
    description.filePattern = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_FILE_PATTERN, "");
    return true;
}

bool FileListCollector::getBSRawFileMetaDir(config::ResourceReader* resourceReader, const std::string& dataTableName,
                                            uint32_t generationId, std::string& metaDir)
{
    vector<string> clusterNames;
    if (!resourceReader->getAllClusters(dataTableName, clusterNames)) {
        string errorMsg = "getClusterNames for " + resourceReader->getConfigPath() + " failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + resourceReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    string indexRoot = buildServiceConfig.getIndexRoot();
    metaDir = IndexPathConstructor::getGenerationIndexPath(indexRoot, clusterNames[0], generationId);
    return true;
}

bool FileListCollector::parseRawDocumentPaths(vector<string>& paths, const string& rawDocumentDir)
{
    vector<string> pathList = StringUtil::split(rawDocumentDir, RAW_DOCUMENT_PATH_SEPERATOR, true);
    for (size_t i = 0; i < pathList.size(); ++i) {
        StringUtil::trim(pathList[i]);
        bool exist = false;
        if (!fslib::util::FileUtil::isExist(pathList[i], exist) || !exist) {
            string errorMsg = "Failed to check whether path[" + pathList[i] + "] is exist";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }
        paths.push_back(pathList[i]);
    }
    if (paths.empty()) {
        string errorMsg = "parse rawdoc dir[" + rawDocumentDir + "] failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool FileListCollector::collectAllResults(const CollectorDescription& desc, CollectResults& result)
{
    vector<string> rawDocPaths = desc.paths;
    sort(rawDocPaths.begin(), rawDocPaths.end());
    uint32_t startId = 0;
    vector<CollectResults> resultsVec;
    resultsVec.reserve(rawDocPaths.size());
    vector<FilePatterns> filePatternsVec;
    filePatternsVec.reserve(rawDocPaths.size());

    for (size_t i = 0; i < rawDocPaths.size(); i++) {
        fslib::FileList candidates;
        if (!getAllCandidateFiles(rawDocPaths[i], candidates)) {
            string errorMsg = "getAllCandidateFiles from [" + rawDocPaths[i] + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        sort(candidates.begin(), candidates.end());
        CollectResults tmpResult;
        tmpResult.reserve(candidates.size());
        for (size_t j = 0; j < candidates.size(); j++) {
            tmpResult.push_back(CollectResult(startId++, candidates[j]));
        }

        FilePatterns filePatterns = FilePatternParser::parse(desc.filePattern, rawDocPaths[i]);

        if (filePatterns.empty()) {
            string errorMsg = "parse file pattern failed!";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return false;
        }

        calculateHashIds(filePatterns, tmpResult);
        resultsVec.push_back(tmpResult);

        filePatternsVec.push_back(filePatterns);
    }

    calculateFileIds(filePatternsVec, resultsVec, result);
    return true;
}

bool FileListCollector::getCollectResultsFromFile(const KeyValueMap& kvMap, CollectResults& result)
{
    string key = getValueFromKeyValueMap(kvMap, DATA_DESCRIPTION_KEY);
    string metaDir = getValueFromKeyValueMap(kvMap, BS_RAW_FILE_META_DIR);
    if (metaDir.empty()) {
        BS_LOG(INFO, "BS_RAW_FILE_META_DIR not in kvMap!");
        return false;
    }
    string metaFile = fslib::util::FileUtil::joinFilePath(metaDir, BS_RAW_FILES_META);
    string content;
    if (!fslib::util::FileUtil::readFile(metaFile, content)) {
        BS_LOG(INFO, "read meta file: [%s] failed", metaFile.c_str());
        return false;
    }
    DescriptionToCollectResultsMap map;
    try {
        FromJsonString(map, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse meta content [%s] fail", content.c_str());
        return false;
    }
    auto iter = map.find(key);
    if (iter != map.end()) {
        BS_LOG(INFO, "use meta file as list");
        result = iter->second;
        return true;
    }
    BS_LOG(INFO, "not use meta file as list");
    return false;
}

bool FileListCollector::collect(const KeyValueMap& kvMap, const CollectorDescription& desc, CollectResults& result)
{
    CollectResults allResults;
    if (!getCollectResultsFromFile(kvMap, allResults)) {
        if (!collectAllResults(desc, allResults)) {
            return false;
        }
    }

    string fileListStr = "";
    uint16_t from = desc.rangeFrom, to = desc.rangeTo;
    for (size_t i = 0; i < allResults.size(); i++) {
        uint16_t hashId = allResults[i]._rangeId;
        if (from <= hashId && hashId <= to) {
            result.push_back(allResults[i]);
            fileListStr += allResults[i].toString();
            fileListStr += ", ";
        }
    }
    BS_LOG(INFO, "file list : %s", fileListStr.c_str());

    return true;
}

void FileListCollector::calculateHashIds(const FilePatterns& filePatterns, CollectResults& result)
{
    CollectResults ret;
    for (size_t i = 0; i < filePatterns.size(); i++) {
        CollectResults onePatternResult = filePatterns[i]->calculateHashIds(result);
        ret.insert(ret.end(), onePatternResult.begin(), onePatternResult.end());
    }
    ret.swap(result);
}

void FileListCollector::calculateFileIds(const vector<FilePatterns>& filePatternsVec,
                                         const vector<CollectResults>& resultsVec, CollectResults& allResults)
{
    FilePatterns allPatterns;
    for (size_t i = 0; i < filePatternsVec.size(); i++) {
        allPatterns.insert(allPatterns.end(), filePatternsVec[i].begin(), filePatternsVec[i].end());
    }
    for (size_t i = 0; i < resultsVec.size(); i++) {
        allResults.insert(allResults.end(), resultsVec[i].begin(), resultsVec[i].end());
    }
    if (needShuffleByHashId(allPatterns)) {
        BS_LOG(INFO, "shuffle collect results by hashid");
        stable_sort(allResults.begin(), allResults.end(), RangeIdComp());
    }
    uint32_t fileId = 0;
    for (size_t i = 0; i < allResults.size(); i++) {
        allResults[i]._globalId = fileId++;
    }
}

bool FileListCollector::needShuffleByHashId(const FilePatterns& filePatterns)
{
    assert(filePatterns.size() > 0);
    if (filePatterns.size() <= 1) {
        return false;
    }
    const FilePatternBasePtr& base = filePatterns[0];
    for (size_t i = 1; i < filePatterns.size(); i++) {
        if (!FilePatternBase::isPatternConsistent(base, filePatterns[i])) {
            return false;
        }
    }
    return true;
}

bool FileListCollector::getAllCandidateFiles(const string& path, fslib::FileList& result)
{
    bool fileExist = false;
    if (!fslib::util::FileUtil::isFile(path, fileExist)) {
        BS_LOG(ERROR, "check whether [%s] is file failed.", path.c_str());
        return false;
    }
    if (fileExist) {
        result.push_back(path);
        return true;
    } else {
        return fslib::util::FileUtil::listDirRecursiveWithAbsolutePath(path, result, true);
    }
}

}} // namespace build_service::reader
