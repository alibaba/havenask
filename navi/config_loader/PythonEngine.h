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
#ifndef NAVICONFIG_PYTHONENGINE_H
#define NAVICONFIG_PYTHONENGINE_H

#include <python3.6m/Python.h>
#include <string>

namespace navi {

class PythonEngine
{
public:
    PythonEngine(const std::string &naviPythonPath);
    ~PythonEngine();
private:
    PythonEngine(const PythonEngine &);
    PythonEngine &operator=(const PythonEngine &);
public:
    bool exec(const std::string &configLoader,
              const std::string &configPath,
              const std::string &loadParam,
              std::string &result);
private:
    static std::string getExternalDir(const std::string &naviPythonPath);
    static std::string getErrorInfo();
    static std::string getObjectStr(PyObject *obj);
    static std::string getString(const char *str);
private:
    std::wstring _path;
private:
    static std::string NAVI_CONFIG_INIT_MODULE;
    static std::string NAVI_CONFIG_INIT_FUNCTION;
};

}

#endif //NAVICONFIG_PYTHONENGINE_H
