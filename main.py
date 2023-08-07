import asyncio
import logging
from typing import Any
import reactivex
from reactivex import operators as ops, Subject
from reactivex.scheduler.eventloop import AsyncIOScheduler
from tornado import web, options, websocket
from message_formatter import process, Message
from db import save_message
from stock_stream import connect

options.define("port", default=8080, help="run on the given port", type=int)


class Application(web.Application):
	def __init__(self):
		handlers = [(r"/stream", StreamSocketHandler)]
		super().__init__(handlers)
		logging.info('Application is running')

		loop = asyncio.get_event_loop()

		connect(loop, stock="BTC-USD").pipe(
			process,
			lambda msg: reactivex.merge(msg, save_message(msg)),
			ops.observe_on(AsyncIOScheduler(loop=loop))
		).subscribe(on_next=StreamSocketHandler.send_updates, on_error=print)


class StreamSocketHandler(websocket.WebSocketHandler):
	waiters: set[Any] = set()

	def open(self):
		logging.info('Hello friend')
		StreamSocketHandler.waiters.add(self)

	def on_close(self):
		StreamSocketHandler.waiters.remove(self)

	def on_message(self, message):
		logging.info("message: {}".format(message))

	@classmethod
	def send_updates(cls, msg: Message):
		logging.info("sending message to %d waiters", len(cls.waiters))
		for waiter in cls.waiters:
			try:
				waiter.write_message({
					"Date": msg.date,
					"Stock_Name": msg.stock,
					"Price": msg.price,
					"Processed_time": msg.processed_timestamp
				}, binary=False)
				print('sent')
			except:
				logging.error("Error sending message", exc_info=True)

async def main():
	options.parse_command_line()
	app = Application()
	app.listen(8080)
	await asyncio.Event().wait()

if __name__ == "__main__":
	asyncio.run(main())