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
#ifndef FSLIB_ERRORGENERATOR_H
#define FSLIB_ERRORGENERATOR_H

#include "autil/Log.h"
#include "autil/Lock.h"
#include "fslib/fslib.h"
#include "fslib/util/Singleton.h"

FSLIB_BEGIN_NAMESPACE(fs);

struct FileSystemError {
    FileSystemError()
        : ec(EC_OK)
        , offset(0)
        , delay(0)
        , retryCount(0)
        , until(0)
    {
    }
    ErrorCode ec;
    uint64_t offset;
    uint64_t delay;
    uint32_t retryCount;
    uint32_t until;
};

class ErrorTrigger
{
public:
    ErrorTrigger(size_t normalIOCount)
        : mNormalIOCount(normalIOCount)
        , mIOCount(0)
    {}
    ~ErrorTrigger() {}
    bool triggerError();

private:
    size_t mNormalIOCount;
    size_t mIOCount;
    mutable autil::ThreadMutex mLock;
};
FSLIB_TYPEDEF_AUTO_PTR(ErrorTrigger);

class ErrorGenerator : public util::Singleton<ErrorGenerator>
{
public:
    friend class util::LazyInstantiation;
    typedef std::map<std::pair<std::string, std::string>, FileSystemError> FileSystemErrorMap;
    typedef std::map<std::string, uint32_t> PathVisitCount;

public:
    typedef std::map<std::pair<std::string, std::string>, uint32_t> MethodVisitCount;

public:
    ~ErrorGenerator();

public:
    bool needGenerateError() const { return _needGenerateError; }
    void openErrorGenerate() { _needGenerateError = true; }
    void shutDownErrorGenerate() { _needGenerateError = false; }
    void setErrorTrigger(ErrorTriggerPtr& errorTrigger)
    { _errorTrigger.reset(errorTrigger.release()); }
    ErrorCode generateFileSystemError(const std::string& operate,
            const std::string& dest);

    ErrorCode generateFileError(const std::string& operate, const std::string& targetPath,
                                uint64_t offset);
    static ErrorGenerator* getInstance();
    void clearMethodVisitCounter();
    MethodVisitCount getMethodVisitCounter();
    void setErrorMap(FileSystemErrorMap map);
    void resetPathVisitCount();

    static const size_t NO_EXCEPTION_COUNT;

private:
    void init();
    bool parseErrorString(const std::string& errStr, std::string& method,
                          FileSystemError& fsError) const;
    void doDelay(uint64_t delay);
    bool needRetry(const std::string& parsePath, uint32_t retry);
    bool reachUntil(const std::string& parsePath, uint32_t until);
    FileSystemErrorMap::const_iterator getError(
            const std::string& operate, const std::string& parsePath);

private:
    ErrorGenerator();
    ErrorGenerator(const ErrorGenerator&);
    ErrorGenerator& operator=(const ErrorGenerator&);

private:
    friend class ErrorGeneratorTest;

private:
    FileSystemErrorMap _fileSystemErrorMap;
    PathVisitCount _pathVisitCount;
    MethodVisitCount _methodVisitCounter;
    bool _needGenerateError;
    ErrorTriggerPtr _errorTrigger;
    autil::ThreadMutex mMutex;

};

FSLIB_TYPEDEF_AUTO_PTR(ErrorGenerator);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_ERRORGENERATOR_H
