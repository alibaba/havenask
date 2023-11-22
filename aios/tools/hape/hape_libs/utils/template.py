import json

class TemplateRender:
    @staticmethod          
    def render(content, render_vars):
        for key in render_vars:
            placeholder = "${"+key+"}"
            value = render_vars[key]
            index = content.find(placeholder)
            if index != -1:
                # print("render {} with {}".format(content, value))
                content = content.replace(placeholder, value)
        return content
    
    @staticmethod
    def render_dict(data_json, render_vars):
        data = json.dumps(data_json)
        data = TemplateRender.render(data, render_vars)
        return json.loads(data)

