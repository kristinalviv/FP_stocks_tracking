import sqlite3
import uuid
from sqlite3 import Connection, Row, Cursor
from typing import Callable
import expression
import reactivex
from reactivex import scheduler, Observable, operators as ops
from message_formatter import Message

scheduler = scheduler.ThreadPoolScheduler(1)
connection_observable: Observable[Connection] = (reactivex.from_callable(lambda: sqlite3.connect('test.db'))
												 .pipe(ops.subscribe_on(scheduler)))


def execute(cmd: str) -> Callable[[Observable[Connection]], Observable[Connection]]:
	return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(cmd)),
									  ops.map(lambda cu: cu.connection))


def query(query_string: str) -> Callable[[Observable[Connection]], Observable[Row]]:
	return lambda cn_obs: cn_obs.pipe(ops.flat_map(_ex(query_string)),
									  ops.flat_map(reactivex.from_iterable))


def commit() -> Callable[[Observable[Connection]], Observable[Connection]]:
	return lambda cn_obs: cn_obs.pipe(ops.map(lambda c: c.commit() or c))


@expression.curry(1)
def _ex(query_string: str, cn: Connection) -> Observable[Cursor]:
	return reactivex.defer(lambda s: reactivex.start(lambda: cn.execute(query_string), s))


def process_msg(c: Connection, msg: Message):
	sql_string = f"SELECT price FROM Stock_Data WHERE stock_name = '{msg.stock}' order by processed_time desc limit 1 "
	db_res = c.execute(sql_string).fetchone()
	db_result = db_res[0]
	if db_result != msg.price:
		print(f"'{msg.stock}' price was changed. Old: {db_result}, new: {msg.price}")
	print(f'Event inserting - {msg.price}')
	c.execute(f"INSERT INTO Stock_Data (id, date, processed_time, price, stock_name ) VALUES"
			  f" ('{uuid.uuid4()}', '{msg.date}', '{msg.processed_timestamp}' ,'{msg.price}', '{msg.stock}')")
	c.commit()


def save_message(source: Observable[Message]) -> Observable:
	return (connection_observable.pipe(
		execute(
			'''CREATE TABLE IF NOT EXISTS Stock_Data
			   (id varchar(48),
			   date varchar(32),
				processed_time long,
				price float,
				stock_name varchar(128))'''),
		ops.flat_map(
			lambda connect: source.pipe(ops.observe_on(scheduler),
										ops.map(lambda msg: process_msg(connect, msg)))),
		ops.ignore_elements()
	))
