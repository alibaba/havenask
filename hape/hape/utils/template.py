import json
import os
import shutil

class JsonTemplateRender:
    @staticmethod
    def render(templ, render_vars):
        if type(templ) == dict: 
            result = {}
            for key in templ:
                result[key] = JsonTemplateRender.render_field(templ[key], render_vars)
        elif type(templ) == list:
            result = []
            for elem in templ:
                result.append(JsonTemplateRender.render_field(elem, render_vars))
        else:
            raise TypeError
        return result

    @staticmethod
    def render_field(field, render_vars):
        # print("type is {}".format(type(field)))
        if type(field) == dict:
            new_field = {}
            for key in field:
                new_field[JsonTemplateRender.render_content(key, render_vars)] = JsonTemplateRender.render_content(field[key], render_vars)
            return new_field
        elif type(field) == list:
            return [JsonTemplateRender.render_content(value, render_vars) for value in field]
        elif type(field) == str or type(field) == unicode:
            return JsonTemplateRender.render_content(field, render_vars)
        else:
            return field
                
    @staticmethod          
    def render_content(content, render_vars):
        if type(content) != str and type(content) != unicode:
            return content
        for key in render_vars:
            placeholder = "${"+key+"}"
            value = render_vars[key]
            index = content.find(placeholder)
            if index != -1:
                # print("render {} with {}".format(content, value))
                content = content.replace(placeholder, value)
        return content


class ConfigDirRender:
    def __init__(self, biz_config_path, render_env):
        self._render_env = render_env
        self._biz_config_path = biz_config_path
        
        
    def render(self):
        self.render_path(True)
        self.render_path(False)
        self.render_content()
        
    def render_path(self, is_dir):
        paths = []
        for root, dirs, files in os.walk(self._biz_config_path, topdown=False):
            if is_dir:
                for name in dirs:
                    paths.append(os.path.join(root, name))
            else:
                for name in files:
                    paths.append(os.path.join(root, name))
                
        for path in paths:
            # print(path)
            for key in self._render_env:
                if path.find(key)!=-1:
                    # print("move {}", path)
                    shutil.move(path, path.replace(key, str(self._render_env[key])))
    
    
    def render_content(self):
        paths = []
        for root, dirs, files in os.walk(self._biz_config_path, topdown=False):
            for name in files:
                # print(name)
                paths.append(os.path.join(root, name))
        for path in paths:
            with open(path) as f:
                content = f.read()
                for key in self._render_env:
                    if content.find(key)!=-1:
                        # print(type(content), content)
                        content = content.replace(key, str(self._render_env[key]))
            with open(path, "w") as f:
                f.write(content)
