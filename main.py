import asyncio

from recorder.configuration import load_config
from recorder.recorder import LOBRecorder

from recorder.db import create_table

if __name__ == '__main__':

    config = load_config()

    create_table(config)

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    recorder = LOBRecorder(loop=loop, config=config)

    loop.run_until_complete(recorder.run())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.close()
