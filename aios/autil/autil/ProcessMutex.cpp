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
#include "autil/ProcessMutex.h"

#include <map>
#include <mutex>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/result/Errors.h"

using namespace std;
using namespace autil::result;
namespace fs = std::filesystem;

namespace autil {

AUTIL_DECLARE_AND_SETUP_LOGGER(autil, ProcessMutex);

struct ProcessMutexFile;
using ProcessMutexFilePtr = shared_ptr<ProcessMutexFile>;
using ProcessMutexFileLookup = map<string, ProcessMutexFilePtr>;

namespace {
template <typename Fn>
struct ScopeExit {
public:
    ScopeExit(Fn &&fn) : fn{std::move(fn)} {}
    ~ScopeExit() { fn(); }

private:
    Fn fn;
};
} // namespace

struct ProcessMutexFile {
public:
    mutex mut;
    int fd;

    static Result<ProcessMutexFilePtr> open(const fs::path &path) {
        int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
        if (fd < 0) {
            return RuntimeError::make(
                "open file [%s] failed, errno [%d]: [%s]", path.c_str(), errno, ::strerrorname_np(errno));
        }

        AUTIL_LOG(INFO, "acquire [%s] with fd [%d]", path.c_str(), fd);
        int err = ::flock(fd, LOCK_EX);
        if (err) {
            return RuntimeError::make("flock file [%s] failed with error code: [%d]", path.c_str(), err);
        }

        static int pid = ::getpid();
        thread_local int tid = ::gettid();
        auto info = std::to_string(pid) + ":" + std::to_string(tid);
        int ret = ::write(fd, info.c_str(), info.size());
        (void)ret;

        auto file = make_shared<ProcessMutexFile>();
        file->fd = fd;
        return file;
    }
};

Result<ProcessMutex> ProcessMutex::acquire(const fs::path &path_) {
    static mutex mut;
    static ProcessMutexFileLookup files;

    auto path = fs::weakly_canonical(path_);
    auto key = path.string();

    typename ProcessMutexFileLookup::iterator it;
    ProcessMutexFilePtr file;
    {
        unique_lock lock{mut};
        it = files.find(key);
        if (it == files.end()) {
            it = files.emplace(key, AR_RET_IF_ERR(ProcessMutexFile::open(path))).first;
        }
        // inc use count
        file = it->second;
    }

    unique_lock lock{file->mut};
    auto release = [it, file = std::move(file), lock = std::move(lock)]() mutable {
        unique_lock _{mut};
        file.reset();
        // check use count
        if (it->second.use_count() > 1) {
            return;
        }
        // release file
        int fd = it->second->fd;
        (void)::flock(fd, LOCK_UN);
        (void)::close(fd);
        files.erase(it);
    };

    return ProcessMutex(make_shared<ScopeExit<decltype(release)>>(std::move(release)));
}

} // namespace autil
