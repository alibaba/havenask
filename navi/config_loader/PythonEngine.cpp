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
#include "navi/config_loader/PythonEngine.h"
#include "autil/StringUtil.h"
#include "navi/log/NaviLogger.h"
#include <cstdlib>

namespace navi {

std::string PythonEngine::NAVI_CONFIG_INIT_MODULE = "navi_config";
std::string PythonEngine::NAVI_CONFIG_INIT_FUNCTION = "navi_config";

PythonEngine::PythonEngine(const std::string &naviPythonPath) {
    auto externalDir = getExternalDir(naviPythonPath);
    _path = std::wstring(externalDir.begin(), externalDir.end());
    NAVI_KERNEL_LOG(INFO, "python path: %s", externalDir.c_str());
    Py_SetPath(_path.c_str());
    Py_SetPythonHome(const_cast<wchar_t *>(_path.c_str()));
    Py_Initialize();
}

PythonEngine::~PythonEngine() {
    Py_Finalize();
}

bool PythonEngine::exec(const std::string &configLoader,
                        const std::string &configPath,
                        const std::string &loadParam,
                        std::string &result)
{
    auto scriptName = PyUnicode_DecodeFSDefault(NAVI_CONFIG_INIT_MODULE.c_str());
    auto initModule = PyImport_Import(scriptName);
    Py_DECREF(scriptName);
    if (!initModule) {
        NAVI_KERNEL_LOG(ERROR, "import module [%s] failed, error [%s]",
                        NAVI_CONFIG_INIT_MODULE.c_str(),
                        getErrorInfo().c_str());
        return false;
    }
    bool success = true;
    auto func = PyObject_GetAttrString(initModule, NAVI_CONFIG_INIT_FUNCTION.c_str());
    if (func && PyCallable_Check(func)) {
        auto args = PyTuple_New(3);
        auto pyLoader = PyUnicode_DecodeFSDefault(configLoader.c_str());
        auto pyConfigPath = PyUnicode_DecodeFSDefault(configPath.c_str());
        auto pyLoaderParam = PyUnicode_DecodeFSDefault(loadParam.c_str());
        PyTuple_SetItem(args, 0, pyLoader);
        PyTuple_SetItem(args, 1, pyConfigPath);
        PyTuple_SetItem(args, 2, pyLoaderParam);
        auto value = PyObject_CallObject(func, args);
        Py_DECREF(args);
        if (value) {
            const char *resultStr = "";
            const char *msgStr = "";
            int pySuccess = 0;
            PyArg_ParseTuple(value, "pss", &pySuccess, &resultStr, &msgStr);
            result = getString(resultStr);
            auto msg = getString(msgStr);
            if (0 == pySuccess) {
                NAVI_KERNEL_LOG(
                    ERROR, "exec python func [%s] failed, error [%s]",
                    NAVI_CONFIG_INIT_FUNCTION.c_str(), msg.c_str());
                success = false;
            } else if (!msg.empty()) {
                NAVI_KERNEL_LOG(WARN, "exec python func [%s] warn [%s]",
                                NAVI_CONFIG_INIT_FUNCTION.c_str(), msg.c_str());
            }
        } else {
            NAVI_KERNEL_LOG(ERROR, "exec python func [%s] failed, error [%s]",
                            NAVI_CONFIG_INIT_FUNCTION.c_str(),
                            getErrorInfo().c_str());
            success = false;
        }
        Py_XDECREF(value);
    } else {
        NAVI_KERNEL_LOG(
            ERROR,
            "no callable python func [%s] exist in module [%s], error [%s]",
            NAVI_CONFIG_INIT_FUNCTION.c_str(), NAVI_CONFIG_INIT_MODULE.c_str(),
            getErrorInfo().c_str());
        success = false;
    }
    Py_DECREF(initModule);
    Py_XDECREF(func);
    return success;
}

std::string PythonEngine::getExternalDir(const std::string &naviPythonPath) {
    // for test
    std::vector<std::string> retValues;
    if (!naviPythonPath.empty()) {
        retValues.push_back(naviPythonPath);
    }
    auto homeStr = ::getenv("NAVI_PYTHON_HOME");
    if (homeStr) {
        retValues.emplace_back(homeStr);
    }
    auto ldLibraryPathRaw = ::getenv("LD_LIBRARY_PATH");
    std::string libraryPath = ldLibraryPathRaw != nullptr ? ldLibraryPathRaw : "";

    std::string suffix("/usr/local/lib64");
    std::vector<std::string> paths = autil::StringUtil::split(libraryPath, ":");
    for (auto path : paths) {
        if (path == suffix) {
            continue;
        }
        path.erase(path.find_last_not_of('/') + 1);
        if (autil::StringUtil::endsWith(path, suffix)) {
            path.resize(path.size() - suffix.size());
            auto pythonPath = path + "/usr/lib64/python3.6/";
            retValues.emplace_back(pythonPath);
            retValues.emplace_back(pythonPath + "lib-dynload/");
            retValues.emplace_back(path + "/usr/local/lib/python/site-packages/navi");
        }
    }
    retValues.emplace_back("/usr/lib64/python3.6/");
    return autil::StringUtil::toString(retValues, ":");
}

std::string PythonEngine::getErrorInfo() {
    PyErr_Print();
    std::string ret;
    PyObject *type = nullptr, *value = nullptr, *trace = nullptr;
    PyErr_Fetch(&type, &value, &trace);
    ret += getObjectStr(type);
    ret += "\n" + getObjectStr(value);
    ret += "\n" + getObjectStr(trace);
    Py_XDECREF(type);
    Py_XDECREF(value);
    Py_XDECREF(trace);
    return ret;
}

std::string PythonEngine::getString(const char *str) {
    if (str) {
        return std::string(str);
    } else {
        return "";
    }
}

std::string PythonEngine::getObjectStr(PyObject *obj) {
    std::string ret;
    if (obj) {
        PyObject *unicode = PyObject_Repr(obj);
        PyObject *pyStr =
            PyUnicode_AsEncodedString(unicode, "utf-8", "ignore");
        auto typeChar = PyBytes_AS_STRING(pyStr);
        if (typeChar) {
            ret = typeChar;
        }
        Py_XDECREF(unicode);
        Py_XDECREF(pyStr);
    }
    return ret;
}

}
