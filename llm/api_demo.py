from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO)

import uvicorn

if __name__ == '__main__':
    uvicorn.run("server.server:app", host="0.0.0.0", port=8000, reload=True)
