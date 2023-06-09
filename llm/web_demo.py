import gradio as gr
import mdtex2html
import inspect
from dotenv import load_dotenv
load_dotenv()

from llm_adapter.factory import get_llm_adapter
from vectorstore.factory import get_vector_store

import logging
logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO)

llm = get_llm_adapter()
store = get_vector_store()

"""Override Chatbot.postprocess"""


def postprocess(self, y):
    if y is None:
        return []
    for i, (message, response) in enumerate(y):
        y[i] = (
            None if message is None else mdtex2html.convert((message)),
            None if response is None else mdtex2html.convert(response),
        )
    return y


gr.Chatbot.postprocess = postprocess


def parse_text(text):
    """copy from https://github.com/GaiZhenbiao/ChuanhuChatGPT/"""
    lines = text.split("\n")
    lines = [line for line in lines if line != ""]
    count = 0
    for i, line in enumerate(lines):
        if "```" in line:
            count += 1
            items = line.split('`')
            if count % 2 == 1:
                lines[i] = f'<pre><code class="language-{items[-1]}">'
            else:
                lines[i] = f'<br></code></pre>'
        else:
            if i > 0:
                if count % 2 == 1:
                    line = line.replace("`", "\`")
                    line = line.replace("<", "&lt;")
                    line = line.replace(">", "&gt;")
                    line = line.replace(" ", "&nbsp;")
                    line = line.replace("*", "&ast;")
                    line = line.replace("_", "&lowbar;")
                    line = line.replace("-", "&#45;")
                    line = line.replace(".", "&#46;")
                    line = line.replace("!", "&#33;")
                    line = line.replace("(", "&#40;")
                    line = line.replace(")", "&#41;")
                    line = line.replace("$", "&#36;")
                lines[i] = "<br>"+line
    text = "".join(lines)
    return text

def reset_user_input():
    return gr.update(value='')

def reset_state():
    return [], []

async def chat(input, chatbot, max_length, top_p, temperature, history):
    chatbot.append((parse_text(input), ""))
    embedding = await llm.embed_query(input)
    candidate_docs = await store.query(embedding, 3)

    if inspect.iscoroutinefunction(llm.stream_chat):
        response = await llm.stream_chat(candidate_docs, input, history, max_length=max_length,
                                         top_p=top_p, temperature=temperature)
    else:
        response = llm.stream_chat(candidate_docs, input, history,
                                   max_length=max_length, top_p=top_p, temperature=temperature)

    async for result, history in response:
        chatbot[-1] = (parse_text(input), parse_text(result))
        yield chatbot, history
    print(f'question: {input}, response: {result}, history: {history}')

with gr.Blocks() as demo:
    gr.HTML("""<h1 align="center">Havenask LLM Web Demo!</h1>""")

    chatbot = gr.Chatbot()
    with gr.Row():
        with gr.Column(scale=4):
            with gr.Column(scale=12):
                user_input = gr.Textbox(show_label=False, placeholder="Input...", lines=10).style(
                    container=False)
            with gr.Column(min_width=32, scale=1):
                submitBtn = gr.Button("Submit", variant="primary")
        with gr.Column(scale=1):
            emptyBtn = gr.Button("Clear History")
            max_length = gr.Slider(0, 4096, value=2048, step=1.0, label="Maximum length", interactive=True)
            top_p = gr.Slider(0, 1, value=0.7, step=0.01, label="Top P", interactive=True)
            temperature = gr.Slider(0, 1, value=0.01, step=0.01, label="Temperature", interactive=True)

    history = gr.State([])

    submitBtn.click(chat, [user_input, chatbot, max_length, top_p, temperature, history], [chatbot, history],
                    show_progress=True)
    submitBtn.click(reset_user_input, [], [user_input])

    emptyBtn.click(reset_state, outputs=[chatbot, history], show_progress=True)

demo.queue().launch(share=False, server_name='0.0.0.0', server_port=7860,inbrowser=True)