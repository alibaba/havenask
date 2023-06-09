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
#include <execinfo.h>
#include <iostream>
#include <link.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <string.h>
#include <cxxabi.h>

namespace autil {
class Backtrace {
public:
    static void Register() {
        signal(SIGSEGV, Backtrace::Callback);
        signal(SIGBUS, Backtrace::Callback);
        signal(SIGFPE, Backtrace::Callback);
        signal(SIGILL, Backtrace::Callback);
        signal(SIGXCPU, Backtrace::Callback);
        signal(SIGXFSZ, Backtrace::Callback);
        signal(SIGSYS, Backtrace::Callback);
    }

    static std::string GetStackTrace() {

        std::stringstream ss;
        void *callstack[MAX_FRAME_COUNT] = {0};
        auto frameCount = backtrace(callstack, MAX_FRAME_COUNT);
        char **messages = backtrace_symbols(callstack, frameCount);
        if (messages == nullptr) {
            ss << "Cannot get backtrace symbols" << std::endl;
            return {};
        }
        ss << "================== Backtrace =================== " << std::endl;
        for (auto i = 3; i < frameCount; i++) {
            ss << "Frame #" << (frameCount - i) << std::endl
               << Backtrace::HandleMessage(messages[i], callstack[i]) << std::endl;
        }
        ss << std::endl;
        free(messages);
        return ss.str();
    }

    static void Callback(int signalNo) {
        std::cout << std::endl
                  << "This process terminated unexpectedly." << std::endl
                  << "Catch signal: [" << strsignal(signalNo) << "]" << std::endl;
        std::cout << Backtrace::GetStackTrace();
        exit(1); // QUIT PROCESS. IF NOT, MAYBE ENDLESS LOOP.
    }

    static std::string HandleMessage(const std::string &message, void *callstack) {
        auto funcName = Backtrace::GetFuncName(message);
        auto binName = Backtrace::GetBinName(message);
        auto addr = Backtrace::GetAddr(message);
        Dl_info info;
        dladdr(callstack, &info);
        auto addr2line = Backtrace::ParseCallstack(info.dli_fname, callstack);

        std::stringstream ss;
        ss << "\tMemAddr:\t" << addr << std::endl;
        ss << "\tFuncName:\t" << funcName << std::endl;
        ss << "\tAddr2line:\t" << addr2line << std::endl;
        return ss.str();
    }

    static std::string GetBinName(const std::string &message) {
        std::string::size_type end = message.find('(');
        if (end == std::string::npos) {
            return "cannot get binary name";
        }
        return message.substr(0, end);
    }

    static std::string ParseCallstack(const char *path, void *addr) {
        char buffer[MAX_BUFFER_LEN];
        char syscom[MAX_BUFFER_LEN];
        Dl_info info;
        link_map *linkMap;
        dladdr1((void *)addr, &info, (void **)&linkMap, RTLD_DL_LINKMAP);
        size_t vma = (size_t)addr - (size_t)(linkMap->l_addr) - 1; // pc一般指向即将执行的代码，因此为了准确定位出错位置，这里需要减1；
        sprintf(syscom, "sh -c 'addr2line -e %s -Ci %zx' 2>/dev/null", path, vma);
        auto p = popen(syscom, "r");
        std::string result;
        while (!feof(p)) {
            if (fgets(buffer, sizeof(buffer), p) != nullptr)
                result += buffer;
        }
        pclose(p);
        if (!result.empty()) {
            result.pop_back();
        }
        result.pop_back();
        return result;
    }

    static std::string GetFuncName(const std::string &message) {
        std::string::size_type begin = message.find('(');
        std::string::size_type end = message.find('+', begin);
        if (begin == std::string::npos || end == std::string::npos) {
            return "cannot get binary name";
        }
        auto mangledName = message.substr(begin + 1, end - begin - 1);
        return Backtrace::GetDemangledName(mangledName);
    }

    static std::string GetDemangledName(const std::string &mangledName) {
        int status;
        char *s = abi::__cxa_demangle(mangledName.c_str(), nullptr, 0, &status);
        if (status != 0) {
            return mangledName;
        }
        std::string demangledName(s);
        free(s);
        return demangledName;
    }

    static std::string GetAddr(const std::string &message) {
        auto end = message.find("[");
        if (end == std::string::npos) {
            return "cannot get addr";
        }

        auto result = message.substr(end + 1);
        if (result.empty()) {
            return result;
        }
        result.pop_back();  // delete "]"
        return result;
    }

    static constexpr int MAX_FRAME_COUNT = 128;
    static constexpr int MAX_BUFFER_LEN = 1024;
};

} // namespace autil
